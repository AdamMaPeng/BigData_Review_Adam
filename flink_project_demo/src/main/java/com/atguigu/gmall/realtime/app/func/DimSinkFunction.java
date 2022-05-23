package com.atguigu.gmall.realtime.app.func;

/**
 * @author Adam-Ma
 * @date 2022/5/23 14:36
 * @Project BigData_Review_Adam
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 * DESC : 将 dim 层的数据，写入到对应的表中
 */

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.util.DruidDSUtil;
import com.atguigu.gmall.realtime.util.PhoenixClientUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;

/**
 *  对于当前项目，可以将 dim 层数据 sink 到
 *      Kafka       ：
 *      Phoenix     ：  支持 jdbc
 *      ClickHouse  ：  支持 jdbc
 *
 *  Flink 提供 JDBCSinkFunction ，但是只能写入到一张表中，所以需要自定义 SinkFunction
 *
 *  由于需要连接到 Phoenix ，所以需要实现 RichSinkFunction ，使用open 声明周期方法，
 *   来获取 数据库连接
 *
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    private DruidDataSource druidDataSource;


    @Override
    public void open(Configuration parameters) throws Exception {
        druidDataSource = DruidDSUtil.createDataSource();
    }

    /**
     *  得到的数据为：
     *      {"name":"玻璃杯","sink_table":"dim_base_category3","id":394,"category2_id":44}
     *      需要将  JsonObj 写入到 对应的 sink_table中
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        // 1、获取表名
        String sink_table = (String) value.get("sink_table");

        // 2、将JSONObject 中的表名剔除，直接得到数据
        value.remove("sink_table");

        // 3、拼接 sql
        String upsertSQL = "upsert into " + GmallConfig.PHOENIX_SCHEMA + "." + sink_table + " ("
                + StringUtils.join(value.keySet(), ",") + ")"
                +  " values ('"  + StringUtils.join(value.values(), "','") + "')" ;

/*
        // JDBC的流程
        // 1、注册驱动
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        // 2、获取连接
        Connection conn = connection;
        // 3、得到数据库对象
        // 4、预编译sql
        PreparedStatement prep = conn.prepareStatement(upsertSQL);
        // 5、执行sql
        prep.executeUpdate();
        // 6、处理结果集
        // 7、关闭资源
        PhoenixClientUtil.close(conn, prep);
 */
        // 获取连接
        Connection connection = druidDataSource.getConnection();
        PhoenixClientUtil.executeSQL(connection, upsertSQL);
        // 执行sql
        // 关闭资源
    }

}
