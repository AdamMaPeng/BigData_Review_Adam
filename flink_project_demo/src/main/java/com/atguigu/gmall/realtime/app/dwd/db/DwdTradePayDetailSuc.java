package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import com.atguigu.gmall.realtime.util.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Adam-Ma
 * @date 2022/5/31 15:59
 * @Project BigData_Review_Adam
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 * DESC : 交易域 支付成功事务事实表
 * 需开启的组件
 *      maxwell ， zk ，Kafka ，DwdTradeOrderProProcess , DwdTradeOrderDetail, DwdTradePayDetailSuc
 * 执行流程
 *      1、读取 DwdTradeOrderDetail 中的数据
 *      2、读取 payment_info 表中的数据
 *      3、读取 base_dic 中的数据
 *      4、将三张表中的数据进行连接，获取到的结果，写入到 DwdTradeOrderDetail 所对应的 topic 中
 */
public class DwdTradePayDetailSuc {
    public static void main(String[] args) {
        // TODO 1.环境准备
        // 1.1 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.2 设置并行度
        env.setParallelism(4);
        // 1.3 流表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2.检查点的设置
        // TODO 3.读取 Kafka dwd_trade_order_detail 中的数据，封装为动态表
        tableEnv.executeSql("" +
                "create table dwd_trade_order_detail(\n" +
                "id string,\n" +
                "order_id string,\n" +
                "user_id string,\n" +
                "sku_id string,\n" +
                "sku_name string,\n" +
                "province_id string,\n" +
                "activity_id string,\n" +
                "activity_rule_id string,\n" +
                "coupon_id string,\n" +
                "date_id string,\n" +
                "create_time string,\n" +
                "source_id string,\n" +
                "source_type_code string,\n" +
                "source_type_name string,\n" +
                "sku_num string,\n" +
                "split_original_amount string,\n" +
                "split_activity_amount string,\n" +
                "split_coupon_amount string,\n" +
                "split_total_amount string,\n" +
                "ts string,\n" +
                "row_op_ts timestamp_ltz(3)\n" +
                ")" + MyKafkaUtil.getKafkaDDL("dwd_trade_order_detail", "dwd_trade_pay_detail_suc"));
//        tableEnv.executeSql("select * from dwd_trade_order_detail").print();

        // TODO 4.读取 Kafka topic_db 中的数据，封装为动态表
        tableEnv.executeSql(MyKafkaUtil.getTopicDbTable("dwdTradePayDetailSuc_Group"));
//        tableEnv.executeSql("select * from topic_db").print();

        // TODO 5.从 topic_db 中过滤出 payment_info 的数据，封装为动态表
        Table paymentInfo = tableEnv.sqlQuery("select\n" +
                        "data['user_id'] user_id,\n" +
                        "data['order_id'] order_id,\n" +
                        "data['payment_type'] payment_type,\n" +
                        "data['callback_time'] callback_time,\n" +
                        "`proc_time`,\n" +
                        "ts\n" +
                        "from topic_db\n" +
                        "where `table` = 'payment_info'\n"
//                +
//                "and `type` = 'update'\n" +
//                "and data['payment_status']='1602'"
        );
        tableEnv.createTemporaryView("payment_info", paymentInfo);
//        tableEnv.executeSql("select * from payment_info").print();

        // TODO 6.读取 mysql 中的 base_dic 中的数据，封装为动态表
        tableEnv.executeSql(MysqlUtil.getBaseDicLoopUpDDL());
//        tableEnv.executeSql("select * from base_dic").print();

        // TODO 7.将 payment_info 、dwd_trade_order_detail、base_dic 中的数据进行连接，封装为动态表
        Table resultTable = tableEnv.sqlQuery("" +
                "select\n" +
                "od.id order_detail_id,\n" +
                "od.order_id,\n" +
                "od.user_id,\n" +
                "od.sku_id,\n" +
                "od.sku_name,\n" +
                "od.province_id,\n" +
                "od.activity_id,\n" +
                "od.activity_rule_id,\n" +
                "od.coupon_id,\n" +
                "pi.payment_type payment_type_code,\n" +
                "dic.dic_name payment_type_name,\n" +
                "pi.callback_time,\n" +
                "od.source_id,\n" +
                "od.source_type_code,\n" +
                "od.source_type_name,\n" +
                "od.sku_num,\n" +
                "od.split_original_amount,\n" +
                "od.split_activity_amount,\n" +
                "od.split_coupon_amount,\n" +
                "od.split_total_amount split_payment_amount,\n" +
                "pi.ts,\n" +
                "od.row_op_ts row_op_ts\n" +
                "from payment_info pi\n" +
                "join dwd_trade_order_detail od\n" +
                "on pi.order_id = od.order_id\n" +
                "left join `base_dic` for system_time as of pi.proc_time as dic\n" +
                "on pi.payment_type = dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);

        // TODO 8.创建 写入 Kafka 支付成功事实表 的topic 对应的动态表
        tableEnv.executeSql("create table dwd_trade_pay_detail_suc(\n" +
                "order_detail_id string,\n" +
                "order_id string,\n" +
                "user_id string,\n" +
                "sku_id string,\n" +
                "sku_name string,\n" +
                "province_id string,\n" +
                "activity_id string,\n" +
                "activity_rule_id string,\n" +
                "coupon_id string,\n" +
                "payment_type_code string,\n" +
                "payment_type_name string,\n" +
                "callback_time string,\n" +
                "source_id string,\n" +
                "source_type_code string,\n" +
                "source_type_name string,\n" +
                "sku_num string,\n" +
                "split_original_amount string,\n" +
                "split_activity_amount string,\n" +
                "split_coupon_amount string,\n" +
                "split_payment_amount string,\n" +
                "ts string,\n" +
                "row_op_ts timestamp_ltz(3),\n" +
                "primary key(order_detail_id) not enforced\n" +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_trade_pay_detail_suc"));

        // TODO 9.将 连接结果写入到 支付成功事实表中，同时查看 对应的 topic 中是否有相应的数据
        tableEnv.executeSql("" +
                "insert into dwd_trade_pay_detail_suc select * from result_table");

        tableEnv.executeSql("select * from dwd_trade_pay_detail_suc").print();
    }
}
