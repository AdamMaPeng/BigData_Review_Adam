package com.atguigu.gmall.realtime.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.util.DruidDSUtil;
import com.atguigu.gmall.realtime.util.PhoenixClientUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Adam-Ma
 * @date 2022/5/22 15:16
 * @Project BigData_Review_Adam
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 * DESC : 业务数据流 连接 配置广播流 ，分别处理 配置信息和 业务数据
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    // 连接池对象
    private DruidDataSource druidDataSource;
    // 广播状态
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    /**
     * 在 open 生命周期中 获取 Phoenix 的 JDBC 连接
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        druidDataSource = DruidDSUtil.createDataSource();
    }

    /**
     *  处理 业务流中的数据
     *      Flink 消费 topic_db 业务流中的数据格式为：
     *      {
     *        "database":"gmall2022",
     *        "table":"comment_info",
     *        "type":"insert",
     *        "ts":1653075671,
     *        "xid":202022,
     *        "xoffset":4502,
     *        "data":{"id":1528098597862608926,"user_id":903,"nick_name":null,"head_img":null,"sku_id":28,"spu_id":9,"order_id":296,"appraise":"1204","comment_txt":"评论内容：78185711691562758441818774946453376644125714471342","create_time":"2022-05-21 03:41:11","operate_time":null}
     *       }
     * @param jsonObj
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processElement(JSONObject jsonObj, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        // 获取状态，判断当前数据中的表名是否为 维度表的表名，是的话，将该数据输出
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        // 获取当前 业务流数据 中的表名
        String tableName = jsonObj.getString("table");
        // 判断当前的表名是否在 广播状态中
        TableProcess tableProcess = broadcastState.get(tableName);
        // 如果 tableProces 不为null
        if (tableProcess != null) {
            // 获取 当前数据的 data 信息
            JSONObject dataJsonObj = jsonObj.getJSONObject("data");
            // 获取 广播状态中，dim 表的 字段信息
            String sinkColumns = tableProcess.getSinkColumns();
            // 过滤掉 dim 表中没有的字段
            filterColumns(dataJsonObj, sinkColumns);

            // 将过滤完 dim 表中不存在字段的数据后，将 数据发往下游，发送前补充发往哪个 dim 表
            String sinkTable = tableProcess.getSinkTable();
            dataJsonObj.put("sink_table", sinkTable);
            out.collect(dataJsonObj);
        }else{
            //从广播状态(配置表中)没有获取到配置信息，说明不是维度
            System.out.println("$$$不是维度，过滤掉" + jsonObj);
        }
    }

    /**
     * 过滤维度表使用不到的字段
     * @param dataJsonObj
     * @param sinkColumns
     */
    private void filterColumns(JSONObject dataJsonObj, String sinkColumns) {
        // 获取 dim 表中所有的字段
        String[] columns = sinkColumns.split(",");
        // 将 dim 表中的字段构成的数组转化为 Array
        List<String> columnList = Arrays.asList(columns);
        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();
        entrySet.removeIf(entry -> !columnList.contains(entry.getKey()));
    }

    /**
     * 处理配置流中的数据
     {
        Flink CDC 采集到的 配置表数据，如下
         "before":null,
         "after":{"source_table":"user_info","sink_table":"dim_user_info1","sink_columns":"id,
                   login_name,name,user_level,birthday,gender,create_time,operate_time","sink_pk":"id","sink_extend":"
                   SALT_BUCKETS = 3"},
         "source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source",
                "ts_ms":1653192938832,"snapshot":"false","db":"gmall2022_config","sequence":null,"table":"table_process",
                "server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},
         "op":"r",
         "ts_ms":1653192938832,
         "transaction":null
     }
     */
    @Override
    public void processBroadcastElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {
        // 将 jsonStr 转换为 JSONObject
        JSONObject jsonObject = JSON.parseObject(jsonStr);
        // 获取到 after 中的数据
        TableProcess tableProcess = jsonObject.getObject("after", TableProcess.class);

        // 获取 dim 的业务表名
        String sourceTable = tableProcess.getSourceTable();
        // 获取存储到 dim 中的表的表名
        String sinkTable = tableProcess.getSinkTable();
        // 获取存储到 dim 中的表的字段
        String sinkColumns = tableProcess.getSinkColumns();
        // 获取存储到 dim 中的表的 pk
        String sinkPk = tableProcess.getSinkPk();
        // 获取存储到 dim 中的表的扩展
        String sinkExtend = tableProcess.getSinkExtend();

        // 通过 配置信息，在 Hbase 中先创建出 dim 层的表
        createDimTable(sinkTable, sinkColumns, sinkPk, sinkExtend);

        // 通过 广播状态将 dim 信息广播出去，主流中的数据可以获取到，做匹配校验
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        broadcastState.put(sourceTable, tableProcess);
    }

    /**
     * 在 Hbase 中创建出对应的 DIM维度表
     * @param tableName
     * @param columnStr
     * @param pk
     * @param ext
     */
    private void createDimTable(String tableName, String columnStr, String pk, String ext) {
        if (ext == null) {
            ext = "";
        }
        if (pk == null) {
            pk = "id";
        }

//        创建一个 StringBuilder 来拼接建表语句
        StringBuilder createTableSQL = new StringBuilder(
                "create table if not exists " + GmallConfig.PHOENIX_SCHEMA + "." + tableName + "("
        );
        // 拼接字段
        StringBuilder createSql = new StringBuilder("create table if not exists " + GmallConfig.PHOENIX_SCHEMA + "." + tableName + "(");

        String[] columnArr = columnStr.split(",");
        for (int i = 0; i < columnArr.length; i++) {
            String column = columnArr[i];
            if (column.equals(pk)) {
                createSql.append(column + " varchar primary key");
            } else {
                createSql.append(column + " varchar");
            }
            //除了最后一个字段后面不加逗号，其它的字段都需要在后面加一个逗号
            if (i < columnArr.length - 1) {
                createSql.append(",");
            }
        }
        createSql.append(") " + ext);
        System.out.println("在phoenix中执行的建表语句: " + createSql);

        try {
            //从连接池中获取连接对象
            Connection conn = druidDataSource.getConnection();
            PhoenixClientUtil.executeSQL(conn, createSql.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
