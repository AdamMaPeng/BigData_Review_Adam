package com.atguigu.gmall.realtime.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Adam-Ma
 * @date 2022/5/22 16:23
 * @Project BigData_Review_Adam
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 * DESC : 创建 Phoenix 客户端，用于执行 建表语句
 */
public class PhoenixClientUtil {

    public static void executeSQL(Connection conn, String sql) {
        PreparedStatement ps = null;
        try {
            // 获取数据库操作对象
            ps = conn.prepareStatement(sql);
            // 执行sql
            ps.execute();
            System.out.println("向 Phoenix 中插入了一条数据：" + sql);
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("执行操作phoenix语句发生了异常");
        }finally {
            close(conn, ps);
        }
    }

    public static void close(Connection conn, PreparedStatement preparedStatement) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
