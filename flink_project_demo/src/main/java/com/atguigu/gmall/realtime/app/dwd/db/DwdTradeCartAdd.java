package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import com.atguigu.gmall.realtime.util.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
import java.time.ZoneId;

/**
 * @author Adam-Ma
 * @date 2022/5/28 11:07
 * @Project BigData_Review_Adam
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 * DESC : 交易域加购事务事实表
 *  需要开启的组件：
 *      maxwell，zk, kafka , [hdfs]
 *  执行流程；
 *      1、maxwell 读取到 mysql binlog中所有的业务 变化的数据，将数据写入到 Kafka 的topic_db 下
 *      2、通过 Flink Sql 从 topic_db 中读取 业务数据 （需要确定 maxwell 发往 topic_db 的数据格式），创建对应的临时表
 *      3、通过 Flink Sql 查询出 只包含 加购的数据
 *            table = cart_info
 *         and 只是加购操作的
 *      4、通过 Flink Sql 读取 mysql 中 base_dic 中的数据，
 *      5、将 加购数据 和 base_dic 进行关联，得到 加购详情数据， 将连接结果 创建为动态表
 *      6、创建 需要写入到 Kafka 中的 交易域加购事务事实表 dwd_trade_cart_add_detail 对应的 Kafka 动态表
 *      7、查询连接结果所有数据 insert into 到 Kafka 交易域加购事务事实表中
 */
public class DwdTradeCartAdd {
    public static void main(String[] args) {
        // TODO 1.环境准备
        // 1.1 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.2 设置并行度
        env.setParallelism(4);
        // 1.3 流表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 设定 Table 中的时区为本地时区
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));
        // 为表关联时状态中存储的数据设置过期时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

        // TODO 设置检查点 （略）

        // TODO 2.通过 Flink SQL ，从 Kafka 中读取 topic_db 数据
        /**
         *  maxwell 采集的数据到 topic_db 中，数据格式为：
         *  {
         *      "database":"gmall2022",
         *      "table":"cart_info",
         *      "type":"update",
         *      "ts":1653223010,
         *      "xid":3214,
         *      "commit":true,
         *      "data":{
         *          "id":30230,"user_id":"4","sku_id":2,"cart_price":6999.00,"sku_num":5,"img_url":"Q404.jpg","sku_name":"小米10游戏手机","is_checked":null,"create_time":"2022-05-22 23:18:51","operate_time":null,"is_ordered":0,"order_time":null,"source_type":"2401","source_id":null},
         *      "old":{"sku_num":3}}
         */
        // 2.1 FlinkSQL: 创建 Kafka topic_db 动态表
        tableEnv.executeSql("create table topic_db (\n" +
                "  `database` string,\n" +
                "  `table`    string,\n" +
                "  `type`     STRING,\n" +
                "  `ts`       string,\n" +
                "  `data`     map<string, string>,\n" +
                "  `old`      map<string, string>,\n" +
                "  `proc_time` as proctime()\n" +
                ") " + MyKafkaUtil.getKafkaDDL("topic_db","cartAddGroup"));
//        tableEnv.executeSql("select * from topicDb").print();
        // 2.2 从业务表中过滤只包含加购的数据
        Table cardAdd = tableEnv.sqlQuery(
            " select \n" +
                "        `data`['id'] id,\n" +
                "        `data`['user_id'] user_id,\n" +
                "        `data`['sku_id'] sku_id,\n" +
                "        `data`['source_type'] source_type,\n" +
                "        `data`['source_id'] source_id,\n" +
                "        if(`type`='insert',\n" +
                "        data['sku_num'],\n" +
                "        cast((cast(data['sku_num'] as int) - cast(`old`['sku_num'] as int)) as string)) sku_num,\n" +
                "        `ts` ts,\n" +
                "        proc_time\n" +
                "from topic_db\n" +
                "where \n" +
                "        `table`='cart_info'\n" +
                "and \n" +
                "    ( \n" +
                "        `type`='insert' \n" +
                "        or\n" +
                "       (`type`='update' and  `old`['sku_num'] is not null and cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int) ) \n" +
                "    ) ");
        // 将查询结果创建为 临时表
        tableEnv.createTemporaryView("cart_add", cardAdd);

        // TODO 3. FlinkSQL : 使用查询缓存（LookUp Cache） 读取 mysql 中的  base_dic 数据
        tableEnv.executeSql(MysqlUtil.getBaseDicLoopUpDDL());
//        tableEnv.executeSql("select * from base_dic").print();

        // TODO 4. 关联两张表获取 加购明细表
        // 5.1 获取最终查询结果
        Table cartAddResult = tableEnv.sqlQuery(
                    "select \n" +
                        "       c.id, c.user_id, c.sku_id, c.source_id, c.source_type, b.dic_name, c.sku_num, c.ts \n" +
                        "from \n" +
                        "        cart_add c\n" +
                        "join    base_dic for system_time as of c.proc_time as b\n" +
                        "on c.source_type = b.dic_code");
        tableEnv.createTemporaryView("cart_add_result", cartAddResult);
//        tableEnv.executeSql("select * from cart_add_result").print();

        // TODO 5. 创建写入 Kafka 中 dwd_trade_cart_add_detail 的 动态表
        tableEnv.executeSql(
                "create table dwd_trade_cart_add_detail (\n" +
                "        id string,\n" +
                "        user_id string,\n" +
                "        sku_id string,\n" +
                "        source_id string,\n" +
                "        source_type string,\n" +
                "        source_type_name string,\n" +
                "        sku_num string,\n" +
                "        ts string,\n" +
                "        primary key(id) not enforced" +
                ") " + MyKafkaUtil.getUpsertKafkaDDL("dwd_trade_cart_add_detail"));

        // 5.2 将最终的结果写入 dwd_trade_cart_add_detail 中
        tableEnv.executeSql("insert into dwd_trade_cart_add_detail select * from cart_add_result");

        tableEnv.executeSql("select * from dwd_trade_cart_add_detail").print();
    }
}
