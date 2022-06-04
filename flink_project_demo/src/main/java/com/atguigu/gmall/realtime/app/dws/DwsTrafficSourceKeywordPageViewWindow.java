package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.app.func.KeywordUDTF;
import com.atguigu.gmall.realtime.bean.KeywordBean;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.util.MyClickHouseUtil;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Adam-Ma
 * @date 2022/6/1 14:27
 * @Project BigData_Review_Adam
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 * DESC : 流量域来源关键词粒度页面浏览各窗口轻度聚合
 *  实现方案： FlinkSQL API
 * 需要开启的组件：
 *      flume , zk, Kafka , [HDFS],DwdTrafficBaseLogSplit, DwsTrafficSourceKeywordPageViewWindow
 * 执行流程：
 *      1）flume 采集神策大数据提供的前端用户埋点日志数据，将日志数据存储到 Kafka 中的 topic_log主题中, 作为 ODS层的日志数据
 *      2）Flink 程序读取 ODS层的 topic_log 中的数据，将topic_log 进行分流，将分流的结果保存到 Kafka 各个流量域主题中，作为 流量域事务事实表
 *          启动日志： dwd_traffic_start_log
 *          页面日志： dwd_traffic_page_log
 *          曝光日志： dwd_traffic_display_log
 *          动作日志： dwd_traffic_action_log
 *          错误日志： dwd_traffic_error_log
 *      3) 由于关键词只是在 页面日志中存在，当前读取Kafka 数据采用FlinkSQL 方式，
 *         所以创建动态表，读取 dwd_traffic_page_log 数据
 *      4） 由于关键词所在页面的 上一页为 search，当前的项目类型为 keyword，项目不为空，所以对当前页面进行判断
 *              last_page_id = search
 *              item_type = keyword
 *              item != null
        5) 根据以上条件进行过滤，过滤出包含关键词的页面
        6）由于需要将搜索内容进行分词操作，当前使用 ik 分词器，编写 ik 分词工具类
        7）由于使用 FlinkSQL，要在 sql 中使用分词功能，直接使用工具类调用方法肯定是不行的。
            所以需要自定义一个函数，用于将搜索内容拆分为多个关键词，类似Hive 中炸裂函数
           所以需要自定义 UDTF (User Definied Table Function)
        8）由于拆分后的每个关键词需要与原来的数据构成一行，所以需要类似 Hive 中的 侧写功能，
            在Flink 官网中的 sql - join 中提供了侧写的使用方式
        9）炸裂，侧写好数据后，需要将数据按照关键词进行分组
        10）将分好组的数据 进行开窗（官网中也提供了 分组后使用窗口函数的方式），聚合统计
        11）由于需要进行开窗，所以需要提前指定水位线，在FlinkSQL中，指定水位线是需要在创建表的时候指定水位线的。
        12）将统计好的数据，写入到 ClickHouse进行存储
             （1） FlinkSQL没有提供与ClickHouse 的连接器，所以需要将 FlinkSQL 中的 Table 转换成 流对象
                  FlinkSQL 中的数据要转换成流，那么在流中以什么类型保存呢，所以就需要提供一个类，用来指明sql中的数据转换成流后以什么类型存在
            （2）转换成流后，将流中的数据写入到ClickHouse 中
                    提供ClickHouse 工具类
 */
public class DwsTrafficSourceKeywordPageViewWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1.环境准备
        // 1.1 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.2 设置并行度
        env.setParallelism(4);
        // 1.3 流表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 1.4 注册自己实现的 UDTF 函数
//        tableEnv.registerFunction("keyWordSplitUDTF",new KeywordUDTF());
        tableEnv.createTemporaryFunction("keywordUDTF", new KeywordUDTF());

        // TODO 2.检查点的设置
        // 2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        // 2.2 设置最大超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        // 2.3 设置两检查点之间的最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        // 2.4 job 取消的时候，检查点是否保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 2.5 重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.milliseconds(3)));
        // 2.6 状态后端
        env.setStateBackend(new HashMapStateBackend());
        // 2.7 检查点保存的位置
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck/dws/DwsTrafficSourceKeywordPageViewWindow");
        // 2.8 将 操作 HDFS 的用户改为 atguigu
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 3.创建临时表读取 dwd_traffic_page_log 中的数据，
        tableEnv.executeSql("create table if not exists dwd_traffic_page_log(\n" +
                "`common` map<string, string>,\n" +
                "`page`   map<string, string>,\n" +
                "`ts`     bigint,\n" +
                "row_time as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)),\n" +
                "WATERMARK FOR row_time AS row_time - INTERVAL '3' SECOND)"
                + MyKafkaUtil.getKafkaDDL("dwd_traffic_page_log", "dws_traffic_source_keyword_page_view_window"));

        tableEnv.executeSql("select * from dwd_traffic_page_log");

        // TODO 4.过滤出 关键词搜索 行为
        // TODO 5.通过 分词函数对搜索关键词进行分词，并且与原表进行拼接
        Table splitTable = tableEnv.sqlQuery("select \n" +
                "keyword,\n" +
                "`ts`,\n" +
                "row_time\n" +
                "from \n" +
                "dwd_traffic_page_log,\n" +
                "LATERAL TABLE(keywordUDTF(`page`['item'])) t(keyword) \n" +
                "where \n" +
                "`page`['last_page_id'] = 'search'\n" +
                "and \n" +
                "`page`['item_type'] = 'keyword'\n" +
                "and \n" +
                "`page`['item'] is not null");
        tableEnv.createTemporaryView("split_table", splitTable);
        tableEnv.executeSql("select * from split_table");

        // TODO 6.将拆分好的关键词进行 分组、 开窗、聚合
        // 6.1 由于需要开窗，所以需要指定 水位线，所以在建表的时候，指定相应的水位线
        Table keywordBeanSearch  = tableEnv.sqlQuery(
                "select\n" +
                    "DATE_FORMAT(TUMBLE_START(row_time, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt,\n" +
                    "DATE_FORMAT(TUMBLE_END(row_time, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt,\n'" +
                    GmallConstant.KEYWORD_SEARCH + "' source,\n" +
                    "keyword,\n" +
                    "count(*) keyword_count,\n" +
                    "UNIX_TIMESTAMP()*1000 ts\n" +
                "from split_table\n" +
                 "GROUP BY TUMBLE(row_time, INTERVAL '10' SECOND),keyword");
        tableEnv.createTemporaryView("keyword_bean_search ", keywordBeanSearch);
//        tableEnv.executeSql("select * from keyword_bean_search").print();

        // TODO 将聚合好的数据写入到 ClickHouse 中
        // TODO 7.将表转换成流 : 使用 tableEnv.toAppendStream（table, Class） ，需要提供转换成流，流中的数据类型是什么？
        DataStream<KeywordBean> keywordBeanDS = tableEnv.toAppendStream(keywordBeanSearch, KeywordBean.class);

        keywordBeanDS.print(">>>>");

        // TODO 8.将流中的数据写入到 ClickHouse 中
        keywordBeanDS.addSink(MyClickHouseUtil.<KeywordBean>getJdbcSink("insert into dws_traffic_source_keyword_page_view_window values(?,?,?,?,?,?)"));

        env.execute();
    }
}
