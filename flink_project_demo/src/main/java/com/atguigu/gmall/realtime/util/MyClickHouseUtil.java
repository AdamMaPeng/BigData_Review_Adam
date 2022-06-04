package com.atguigu.gmall.realtime.util;

import com.atguigu.gmall.realtime.bean.TransientSink;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Adam-Ma
 * @date 2022/6/3 17:48
 * @Project BigData_Review_Adam
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 * DESC : 操作 ClickHouse 工具类
 *  由于每次操作的类型不一样
 */
public class MyClickHouseUtil {
    public static <T>SinkFunction <T>getJdbcSink(String sql){
        SinkFunction<T> sinkFunction = JdbcSink.sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        // 填充占位符
                        /**
                         *  1、 T t 中的属性就和 sql 中的占位符一一对应
                         *  2、t 的属性类型 和 sql 中占位符处字段类型一样
                         */
                        Class<?> tClass = t.getClass();
                        // 获取当前对象的所有字段
                        Field[] fields = tClass.getDeclaredFields();

                        // 定义一个变量，记录一共有多少个注解
                        int annoNum = 0;

                        // 遍历 fields ，将 fields 赋值给对应位置的占位符
                        for (int i = 0; i < fields.length; i++) {
                            // 1、获取属性对象
                            Field field = fields[i];
                            // 2、将属性的设置权限打开
                            field.setAccessible(true);

                            // 获取当前属性是否有 TransientSink 注解
                            TransientSink annotation = field.getAnnotation(TransientSink.class);

                            // 判断当前 annotation 是否为 null ，如果不为null ，则证明当前属性不需要保存到 ClickHouse 中
                            if (annotation != null) {
                                annoNum += 1;
                                continue;
                            }

                            // 3、获取属性的值
                            try {
                                Object fieldValue = field.get(t);
                                // 4、将当前属性的值赋值给对应位置的占位符
                                preparedStatement.setObject(i + 1 - annoNum, fieldValue);
                            } catch (IllegalAccessException e) {
                                System.out.println("ClickHouse 数据插入 SQL 占位符传参异常 ~");
                                e.printStackTrace();
                            }
                        }
                    }
                },
                new JdbcExecutionOptions
                        .Builder()
                        .withBatchSize(5)
                        .withBatchIntervalMs(3000)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions
                        .JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .withUsername(GmallConfig.CLICKHOUSE_USERNAME)
                        .withPassword(GmallConfig.CLICKHOUSE_PASSWORD)
                        .build()
        );

        return sinkFunction;
    }
}
