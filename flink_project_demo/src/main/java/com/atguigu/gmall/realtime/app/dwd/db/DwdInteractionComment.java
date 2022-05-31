package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import com.atguigu.gmall.realtime.util.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Adam-Ma
 * @date 2022/5/31 19:42
 * @Project BigData_Review_Adam
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 * DESC : 互动域评价事务事实表
 *
 * 需要开启的组件：
 *      maxwell ， zk ，kafka ， [hdfs]
 * 执行流程：
 *      直接读取 topic_db 中的数据， 过滤 table=comment_Info , type = insert
 *      将过滤的结果保存为临时表， 获取到 base_dic 表数据，
 *      将两表进行关联 （获取评价等级）
        将最终的数据写入 Kafka 中对应的 topic 中
 */
public class DwdInteractionComment {
    public static void main(String[] args) {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2. 检查点相关设置(略)

        // TODO 3. 从 Kafka 读取 topic_db 数据，封装为 Flink SQL 表
        tableEnv.executeSql(MyKafkaUtil.getTopicDbTable("dwd_interaction_comment"));

        // TODO 4. 读取评论表数据
        Table commentInfo = tableEnv.sqlQuery("select\n" +
                "data['id'] id,\n" +
                "data['user_id'] user_id,\n" +
                "data['sku_id'] sku_id,\n" +
                "data['order_id'] order_id,\n" +
                "data['create_time'] create_time,\n" +
                "data['appraise'] appraise,\n" +
                "proc_time,\n" +
                "ts\n" +
                "from topic_db\n" +
                "where `table` = 'comment_info'\n" +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("comment_info", commentInfo);

        // TODO 5. 建立 MySQL-LookUp 字典表
        tableEnv.executeSql(MysqlUtil.getBaseDicLoopUpDDL());

        // TODO 6. 关联两张表
        Table resultTable = tableEnv.sqlQuery("select\n" +
                "ci.id,\n" +
                "ci.user_id,\n" +
                "ci.sku_id,\n" +
                "ci.order_id,\n" +
                "date_format(ci.create_time,'yyyy-MM-dd') date_id,\n" +
                "ci.create_time,\n" +
                "ci.appraise,\n" +
                "dic.dic_name,\n" +
                "ts\n" +
                "from comment_info ci\n" +
                "left join\n" +
                "base_dic for system_time as of ci.proc_time as dic\n" +
                "on ci.appraise = dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);

        // TODO 7. 建立 Upsert-Kafka dwd_interaction_comment 表
        tableEnv.executeSql("create table dwd_interaction_comment(\n" +
                "id string,\n" +
                "user_id string,\n" +
                "sku_id string,\n" +
                "order_id string,\n" +
                "date_id string,\n" +
                "create_time string,\n" +
                "appraise_code string,\n" +
                "appraise_name string,\n" +
                "ts string,\n" +
                "primary key(id) not enforced\n" +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_interaction_comment"));

        // TODO 8. 将关联结果写入 Upsert-Kafka 表
        tableEnv.executeSql("" +
                "insert into dwd_interaction_comment select * from result_table");

        tableEnv.executeSql("select * from dwd_interaction_comment").print();
    }
}
