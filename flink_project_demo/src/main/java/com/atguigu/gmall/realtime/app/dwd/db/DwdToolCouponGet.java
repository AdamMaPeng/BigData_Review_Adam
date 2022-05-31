package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Adam-Ma
 * @date 2022/5/31 18:33
 * @Project BigData_Review_Adam
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 * DESC : 工具域优惠券领用事务事实表
 *
 * 需要开启的组件：
 *      maxwell, zk, kafka
 * 执行流程：
 *      直接从 topic_db 中获取 table = coupon＿use 的数据 ，保存到 Kafka 对应的 主题中即可
 */
public class DwdToolCouponGet {
    public static void main(String[] args) {
        // TODO 1. 环境准备
        // 1.1 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.2 设置并行度
        env.setParallelism(4);
        // 1.3 流表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 检查点设置
        // 2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        // 2.2 设置超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        // 2.3 job 取消时，检查点是否保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 2.4 两个检查点之间的最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        // 2.5 重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.milliseconds(2L)));
        // 2.6 设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
        // 2.7 设置检查点保存位置
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck/dwd/dwdToolCouponGet");
        // 2.8 设置操作 HDFS 用户为 atguigu
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 3.读取 topic_db 中的数据，封装为 临时表
        tableEnv.executeSql(MyKafkaUtil.getTopicDbTable("dwdToolCouponGet"));

        // TODO 4.从 topic_db 中过滤 出 table = coupon_use 数据，并封装为临时表
        Table resultTable = tableEnv.sqlQuery("select\n" +
                "data['id'],\n" +
                "data['coupon_id'],\n" +
                "data['user_id'],\n" +
                "date_format(data['get_time'],'yyyy-MM-dd') date_id,\n" +
                "data['get_time'],\n" +
                "ts\n" +
                "from topic_db\n" +
                "where `table` = 'coupon_use'\n" +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("result_table", resultTable);

        // TODO 5.创建 KafkaUpsert 表，对应当前的 优惠券领用事务事实表
        tableEnv.executeSql("create table dwd_tool_coupon_get (\n" +
                "id string,\n" +
                "coupon_id string,\n" +
                "user_id string,\n" +
                "date_id string,\n" +
                "get_time string,\n" +
                "ts string,\n" +
                "primary key(id) not enforced\n" +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_tool_coupon_get"));

        // TODO 6.将过滤到的数据，写入到 优惠券领用事务事实表
        tableEnv.executeSql("insert into dwd_tool_coupon_get select * from result_table");

        tableEnv.executeSql("select * from dwd_tool_coupon_get").print();
    }
}
