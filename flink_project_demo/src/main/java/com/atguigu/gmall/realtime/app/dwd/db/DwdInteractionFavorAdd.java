package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Adam-Ma
 * @date 2022/5/31 19:34
 * @Project BigData_Review_Adam
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 * DESC : 互动域收藏商品事务事实表
 *
 * 需要开启的组件：
 *     maxwell ， zk ,kafka ,【hdfs】
 * 执行流程：
 *      直接从 topic_db 中消费数据， table=favor_info ,type =insert
 *      然后将过滤出来的数据，封装为临时表，然后创建 需要写入 kafka中的表，
 *      将过滤出的数据全部插入到 kafka 中的表
 */
public class DwdInteractionFavorAdd {
    public static void main(String[] args) {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 检查点相关设置(略)

        // TODO 3. 从 Kafka 读取 topic_db 数据，封装为 Flink SQL 表
        tableEnv.executeSql(MyKafkaUtil.getTopicDbTable("dwd_interaction_favor_add"));

        // TODO 4. 读取收藏表数据
        Table favorInfo = tableEnv.sqlQuery("select\n" +
                "data['id'] id,\n" +
                "data['user_id'] user_id,\n" +
                "data['sku_id'] sku_id,\n" +
                "date_format(data['create_time'],'yyyy-MM-dd') date_id,\n" +
                "data['create_time'] create_time,\n" +
                "ts\n" +
                "from topic_db\n" +
                "where `table` = 'favor_info'\n" +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("favor_info", favorInfo);

        // TODO 5. 创建 Upsert-Kafka dwd_interaction_favor_add 表
        tableEnv.executeSql("create table dwd_interaction_favor_add (\n" +
                "id string,\n" +
                "user_id string,\n" +
                "sku_id string,\n" +
                "date_id string,\n" +
                "create_time string,\n" +
                "ts string,\n" +
                "primary key(id) not enforced\n" +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_interaction_favor_add"));

        // TODO 6. 将数据写入 Upsert-Kafka 表
        tableEnv.executeSql("" +
                "insert into dwd_interaction_favor_add select * from favor_info");

        tableEnv.executeSql("select * from dwd_interaction_favor_add").print();
    }
}
