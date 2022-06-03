package com.atguigu.gmall.realtime.common;

/**
 * @author Adam-Ma
 * @date 2022/5/22 12:23
 * @Project BigData_Review_Adam
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 * DESC :
 */
public class GmallConfig {
    public static final String KF_BOOTSTRAP_SERVER = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    public static final String PHOENIX_SCHEMA = "GMALL1118_REALTIME";
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    public static final String PHOENIX_URL = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";

    // ClickHouse 驱动
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

    // ClickHouse 连接 URL
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop102:8123/default";
}
