package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

/**
 * @author Adam-Ma
 * @date 2022/5/31 19:47
 * @Project BigData_Review_Adam
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 * DESC : 用户域 用户注册事务事实表
 *
 * 需要开启的组件：
 *      maxwell, zk, kafka
 * 执行流程：
 *      直接读取 topic_db 中的数据， table=user_info , type=insert
 *      将过滤的数据创建为临时表， 创建相应的写入 kafka 中的 topic 表
 *      查询过滤的数据插入到topic 表中
 */
public class DwdUserRegister {
    public static void main(String[] args) {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));

        //TODO 2. 检查点相关设置(略)

        // TODO 3. 从 Kafka 读取 topic_db 数据，封装为 Flink SQL 表
        tableEnv.executeSql(MyKafkaUtil.getTopicDbTable("dwd_user_register_group"));

        // TODO 4. 读取用户表数据
        Table userInfo = tableEnv.sqlQuery("select\n" +
                "data['id'] user_id,\n" +
                "data['create_time'] create_time,\n" +
                "ts\n" +
                "from topic_db\n" +
                "where `table` = 'user_info'\n" +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("user_info", userInfo);

        // TODO 5. 创建 Upsert-Kafka dwd_user_register 表
        tableEnv.executeSql("create table `dwd_user_register`(\n" +
                "`user_id` string,\n" +
                "`date_id` string,\n" +
                "`create_time` string,\n" +
                "`ts` string,\n" +
                "primary key(`user_id`) not enforced\n" +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_user_register"));

        // TODO 6. 将输入写入 Upsert-Kafka 表
        tableEnv.executeSql("insert into dwd_user_register\n" +
                "select \n" +
                "user_id,\n" +
                "date_format(create_time, 'yyyy-MM-dd') date_id,\n" +
                "create_time,\n" +
                "ts\n" +
                "from user_info");

        tableEnv.executeSql("select * from dwd_user_register").print();
    }
}
