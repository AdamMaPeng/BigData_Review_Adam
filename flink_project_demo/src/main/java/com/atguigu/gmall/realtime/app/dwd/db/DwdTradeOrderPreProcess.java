package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import com.atguigu.gmall.realtime.util.MysqlUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
import java.time.ZoneId;

/**
 * @author Adam-Ma
 * @date 2022/5/31 10:42
 * @Project BigData_Review_Adam
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 * DESC : 交易域订单预处理表 （订单宽表）
 *
 * 实现原因： 因为订单明细表 和  取消订单明细表， 只是业务过程不同和过滤条件不同，
 *          为了减少重复计算，所以将两个表的公共关联过程提取出来，形成订单预处理表
 *
 * 构成：  订单表                 order_info
 *        订单明细表              order_detail_info
 *        订单明细活动关联表       order_detail_activity
 *        订单明细优惠券关联表     order_detail_coupon
 *        字典表                  base_dic
 *
 *        最终结果表 ：
 *          order_pre_process =     order_info  oi
 *                              inner join
 *                                  order__detail_info odi
 *                               left join
 *                                  order_detail_activity
 *                               left join
 *                                  order_detail_coupon
 *                               inner join
 *                                  base_dic
 *
 *  数据来源 ： 从 ods 层的 topic_db 过来的数据，需要保留 type 和 old 字段
 *
 *  需要开启的组件：
 *      maxwell 、 zk 、Kafka 、HDFS
 *
 *  实现思路：
 *      1、业务数据库中 有订单变化的数据， 数据变更记录在 mysql  的 binlog 中
 *      2、maxwell 同步 binlog 日志，将变化数据 采集到 Kafka 的 topic_db 主题中
 *      3、通过 Flink SQL 创建动态表 读取 topic_db 中的数据
 *      4、过滤 出 订单明细表 order_detail_info 的数据，将 查询过滤结果 创建为临时视图 order_detail_info （主表） order_id
 *      5、过滤出 订单表 order_info 的数据，将查询过滤的结果创建为临时视图 order_info （获取id, user_id, province_id, type, old 字段）
 *      6、过滤出 订单明细活动表数据   order_detail_activity 的数据，将查询过滤结果创建为临时视图 order_detail_activity （获取 活动 id， 活动规则id，）
 *      7、过滤出 订单明细优惠券表数据 order_detail_coupon   的数据，将查询过滤结果创建为临时视图 order_detail_coupon （获取优惠券 id）
 *      8、mysql LookUp 查询缓存方式读取 base_dic 数据，创建为临时视图 base_dic（获取订单 类型名称 dic_name）
 *      9、将 以上 5 张表的 连接起来，将查询结果 创建类临时视图 result_table
 *      10、创建 Kafka Upsert 动态表 dwd_trade_order_pre_process，将 result_table 中的数据 写入 对应的dwd_trade_order_pre_process  topic 中
 */
public class DwdTradeOrderPreProcess {
    public static void main(String[] args) {
        // TODO 1.环境准备
        // 1.1 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.2 设置并行度
        env.setParallelism(4);
        // 1.3 流表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 1.4 设置状态失效时间，防止乱序
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5L));
        // 1.5 设置当前的本地时间
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));

        // TODO 2. 检查点的设置
        // 2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        // 2.2 检查点最大超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        // 2.3 job 取消时，检查点保存策略
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 2.4 设置两个检查点之间的最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        // 2.5 设置重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.milliseconds(3)));
        // 2.6 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        // 2.7 设置操作 HDFS 的用户为 atguigu
//        env.setDefaultSavepointDirectory("hdfs://hadoop102:8020/gmall/ck/dwd/dwdTradeOrderProProcess");
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 3. 创建动态表 topic_db 读取 Kafka 中的topic_db 中的数据( 建议添加 proc_time as proctime() 时间，方便与 MysqlLookUp查询缓存方式进行 join 的时候，维度数据进行使用)
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
        // 3.1 创建Kafka topic_db对应的临时表topic_db ，获取 topic_db 中的数据
        String groupId = "dwd_trade_order_pre_process";
        tableEnv.executeSql(MyKafkaUtil.getTopicDbTable(groupId));
//        tableEnv.executeSql("select * from topic_db").print();

        // TODO 4. 过滤出 order_detail_info 的信息 ,并将 查询结果创建为临时视图
        Table order_detail = tableEnv.sqlQuery("select  data['id'] id,\n" +
                "data['order_id'] order_id,\n" +
                "data['sku_id'] sku_id,\n" +
                "data['sku_name'] sku_name,\n" +
                "data['create_time'] create_time,\n" +
                "data['source_id'] source_id,\n" +
                "data['source_type'] source_type,\n" +
                "data['sku_num'] sku_num,\n" +
                "cast(cast(data['sku_num'] as decimal(16,2)) * " +
                "cast(data['order_price'] as decimal(16,2)) as String) split_original_amount,\n" +
                "data['split_total_amount'] split_total_amount,\n" +
                "data['split_activity_amount'] split_activity_amount,\n" +
                "data['split_coupon_amount'] split_coupon_amount,\n" +
                "ts od_ts,\n" +
                "proc_time\n" +
                "from `topic_db` where `table` = 'order_detail' " +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("order_detail", order_detail);

        // TODO 5. 过滤出 order_info 的信息， 保留 type 和 old 字段 供后续 订单明细和 取消订单明细使用 并将 查询结果创建为临时视图
        Table order_info = tableEnv.sqlQuery("select \n" +
                "data['id'] id,\n" +
                "data['user_id'] user_id,\n" +
                "data['province_id'] province_id,\n" +
                "data['operate_time'] operate_time,\n" +
                "data['order_status'] order_status,\n" +
                "`type`,\n" +
                "`old`,\n" +
                "ts oi_ts\n" +
                "from `topic_db`\n" +
                "where `table` = 'order_info'\n" +
                "and (`type` = 'insert' or `type` = 'update')");
        tableEnv.createTemporaryView("order_info", order_info);

        // TODO 6. 过滤出 order_detail_activity 的数据， 保留活动 id， 和 活动 规则 id , 并将 查询结果创建为临时视图
        Table order_detail_activity = tableEnv.sqlQuery("select \n" +
                "data['order_detail_id'] order_detail_id,\n" +
                "data['activity_id'] activity_id,\n" +
                "data['activity_rule_id'] activity_rule_id\n" +
                "from `topic_db`\n" +
                "where `table` = 'order_detail_activity'\n" +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("order_detail_activity", order_detail_activity);

        // TODO 7. 过滤出 order_detail_coupon 的数据， 保留 优惠券 id 并将 查询结果创建为临时视图
        Table order_detail_coupon = tableEnv.sqlQuery("select\n" +
                "data['order_detail_id'] order_detail_id,\n" +
                "data['coupon_id'] coupon_id\n" +
                "from `topic_db`\n" +
                "where `table` = 'order_detail_coupon'\n" +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("order_detail_coupon", order_detail_coupon);

        // TODO 8、通过 mysql LookUp 查询缓存方式读取 base_dic 中的数据，创建为临时试图他
        tableEnv.executeSql(MysqlUtil.getBaseDicLoopUpDDL());

        // TODO 9、将以上五张表的数据进行连接，得到连接结果，并创建为临时视图
        Table result_table = tableEnv.sqlQuery("select \n" +
                "od.id,\n" +
                "od.order_id,\n" +
                "oi.user_id,\n" +
                "oi.order_status,\n" +
                "od.sku_id,\n" +
                "od.sku_name,\n" +
                "oi.province_id,\n" +
                "act.activity_id,\n" +
                "act.activity_rule_id,\n" +
                "cou.coupon_id,\n" +
                "date_format(od.create_time, 'yyyy-MM-dd') date_id,\n" +
                "od.create_time,\n" +
                "date_format(oi.operate_time, 'yyyy-MM-dd') operate_date_id,\n" +
                "oi.operate_time,\n" +
                "od.source_id,\n" +
                "od.source_type,\n" +
                "dic.dic_name source_type_name,\n" +
                "od.sku_num,\n" +
                "od.split_original_amount,\n" +
                "od.split_activity_amount,\n" +
                "od.split_coupon_amount,\n" +
                "od.split_total_amount,\n" +
                "oi.`type`,\n" +
                "oi.`old`,\n" +
                "od.od_ts,\n" +
                "oi.oi_ts,\n" +
                "current_row_timestamp() row_op_ts\n" +
                "from order_detail od \n" +
                "join order_info oi\n" +
                "on od.order_id = oi.id\n" +
                "left join order_detail_activity act\n" +
                "on od.id = act.order_detail_id\n" +
                "left join order_detail_coupon cou\n" +
                "on od.id = cou.order_detail_id\n" +
                "join `base_dic` for system_time as of od.proc_time as dic\n" +
                "on od.source_type = dic.dic_code");
        tableEnv.createTemporaryView("result_table", result_table);

        // TODO 10. 创建 KafkaUpsert 临时表，用于将数据写入到 Kafka 对应的 topic 中
        tableEnv.executeSql("" +
                "create table dwd_trade_order_pre_process(\n" +
                "id string,\n" +
                "order_id string,\n" +
                "user_id string,\n" +
                "order_status string,\n" +
                "sku_id string,\n" +
                "sku_name string,\n" +
                "province_id string,\n" +
                "activity_id string,\n" +
                "activity_rule_id string,\n" +
                "coupon_id string,\n" +
                "date_id string,\n" +
                "create_time string,\n" +
                "operate_date_id string,\n" +
                "operate_time string,\n" +
                "source_id string,\n" +
                "source_type string,\n" +
                "source_type_name string,\n" +
                "sku_num string,\n" +
                "split_original_amount string,\n" +
                "split_activity_amount string,\n" +
                "split_coupon_amount string,\n" +
                "split_total_amount string,\n" +
                "`type` string,\n" +
                "`old` map<string,string>,\n" +
                "od_ts string,\n" +
                "oi_ts string,\n" +
                "row_op_ts timestamp_ltz(3),\n" +
                "primary key(id) not enforced\n" +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_trade_order_pre_process"));

        // 将查询结果写入 KafkaUpsert 中的 dwd_trade_order_pre_process 表中
        tableEnv.executeSql("insert into dwd_trade_order_pre_process select * from result_table");

        tableEnv.executeSql("select * from dwd_trade_order_pre_process").print();
    }
}
