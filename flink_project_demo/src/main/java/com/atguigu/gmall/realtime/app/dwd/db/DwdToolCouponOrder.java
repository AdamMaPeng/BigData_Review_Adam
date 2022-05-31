package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Adam-Ma
 * @date 2022/5/31 18:53
 * @Project BigData_Review_Adam
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 * DESC : 工具域 优惠券使用下单事务事实表
 *
 * 需开启的组件：
 *      maxwell ， zk, kafka ,【HDFS】
 * 执行流程
 *      直接 读取 topic_db 中的数据，过滤 table= coupon_use ，type=update，
 *      当使用优惠券的时候，会将优惠券 的 coupon_status 从 【1401 未使用】 -- 》 【1402 使用中】
 */
public class DwdToolCouponOrder {
    public static void main(String[] args) {
        // TODO 1.环境准备
        // 1.1 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.2 设置并行度
        env.setParallelism(4);
        // 1.3 流表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2.检查点

        // TODO 3. 读取 topic_db 中的数据，创建临时表
        tableEnv.executeSql(MyKafkaUtil.getTopicDbTable("dwd_tool_coupon_order"));

        // TODO 4. 过滤出 table=coupon_use ，且type=update, coupon_status=1402,并创建临时表
        Table couponUseOrder = tableEnv.sqlQuery("select\n" +
                "data['id'] id,\n" +
                "data['coupon_id'] coupon_id,\n" +
                "data['user_id'] user_id,\n" +
                "data['order_id'] order_id,\n" +
                "date_format(data['using_time'],'yyyy-MM-dd') date_id,\n" +
                "data['using_time'] using_time,\n" +
                "ts\n" +
                "from topic_db\n" +
                "where `table` = 'coupon_use'\n" +
                "and `type` = 'update'\n" +
                "and data['coupon_status'] = '1402'\n" +
                "and `old`['coupon_status'] = '1401'");

        tableEnv.createTemporaryView("result_table", couponUseOrder);

        // TODO 5.创建 Kafka Upsert 表，
        tableEnv.executeSql("create table dwd_tool_coupon_order(\n" +
                "id string,\n" +
                "coupon_id string,\n" +
                "user_id string,\n" +
                "order_id string,\n" +
                "date_id string,\n" +
                "order_time string,\n" +
                "ts string,\n" +
                "primary key(id) not enforced\n" +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_tool_coupon_order"));

        // TODO 6.将过滤数据写入  Kafka Upsert
        tableEnv.executeSql("" +
                "insert into dwd_tool_coupon_order select " +
                "id,\n" +
                "coupon_id,\n" +
                "user_id,\n" +
                "order_id,\n" +
                "date_id,\n" +
                "using_time order_time,\n" +
                "ts from result_table");

        tableEnv.executeSql("select * from dwd_tool_coupon_order").print();
    }
}
