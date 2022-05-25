package com.atguigu.gmall.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONValidator;
import com.atguigu.gmall.realtime.app.func.DimSinkFunction;
import com.atguigu.gmall.realtime.app.func.TableProcessFunction;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @author Adam-Ma
 * @date 2022/5/21 8:32
 * @Project BigData_Review_Adam
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 * DESC : 保存维度数据
 * <p>
 * 需要开启的组件：
 * 1、db_log    ：  java -jar xxxx.jar 产生 业务数据 到MySQL 中各个表中
 * 2、maxwell   ： 采集mysql 中变化的数据到 kafka 的 topic_db 中
 * 3、zookeeper ： kafka相关的信息保存在 kafka 中
 * 4、kafka     ： 用于存储 maxwell 采集到的业务数据
 * 5、Hadoop    ： 当前保存检查点采用保存在外部文件系统中，此处将检查点保存在 HDFS中
 * 6、Hbase     ： 保存最终的 DIM 层数据
 * <p>
 * <p>
 * 获取维度数据，保存维度数据
 * 流程：
 * 1、环境准备
 * 2、检查点的设置
 * 3、读取数据源
 * 3.1 维度配置数据
 * 3.1.1 读取 dim 维度相关的配置表信息 -- FlinkCDC 采集配置表中的数据
 * 3.1.2 将dim 配置数据进行广播 -- 广播流
 * 3.2 读取 Kafka 中的业务数据
 * 3.2.1 读取 topic_db 中的数据 -- jsonStr
 * 3.2.2 将 jsonStr --> jsonObj
 * 4、将 广播流（dim配置数据）与 主流（业务数据）连接-- connect
 * 5、通过广播流中的 dim 配置数据 在 Hbase 中创建 dim 表
 * 6、将 dim 配置数据 设置为广播状态，供主流中的业务数据匹配相应的维度数据
 * 7、将 维度数据写入 Hbase 表中
 */

public class DimSinkApp {
    public static void main(String[] args) throws Exception {
        // TODO 1.流执行环境准备
        //1.1 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        /*
         *  1、该并行度必须与 读取的 Kafka 的topic 中的分区数量相同，当前项目中，所有的topic 设置的分区数为 4
         *  通过对当前环境全局的并行度，在读取kafka中的数据时，构成一个消费者组，并行度中的每个并行子任务相当于一个消费者
         *  一个消费者消费一个分区的数据。
         *
         *  2、如果并行度小于分区数，假设并行度=2， 分区数=4， 当使用 keyBy这样的算子时，
         */
        env.setParallelism(4);

        // TODO 2.设置检查点
        // 2.1 开启检查点
        /*
         *  CheckpointingMode.EXACTLY_ONCE
         *      指的是 检查点分界线 barrier 。
         *      1、如果是 EXACTLY_ONCE  ，则表名所有的检查点分界线都到齐，才会执行当前算子状态快照，精确一次的状态一致性
         *      2、如果是 AT_LEAST_ONCE , 则表明至少 有一个 检查点到了后，就会保存当前算子状态快照，至少一次，效率高，但是精确性差
         */
        //env.enableCheckpointing(5000L, CheckpointingMode.AT_LEAST_ONCE);
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        // 2.2 两个检查点之间最小的时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        // 2.3 最大超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        // 2.4 job取消后，检查点是否保存
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 2.5 重启策略
        /**
         *  检查点的容错特性，如果在重试的过程中网络超时了，超时 1min（当前设置），自动进行重启
         *  fixedDelayRestart  ： 当 365 天总共 重启了3 次，就直接报错
         *  failureRateRestart ： 故障率重启，30天内重启3次，每隔3秒重启一次
         */
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(3)));
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(3L)));
        // 2.6 状态后端
        /**
         *  对于状态后端：
         *      Flink 1.12 时，存在三种状态后端
         *          Memory StateBacked 基于内存的状态后端，状态存储在TaskManager 中，检查点保存在 JobManager 中
         *          FsStateBacked       基于文件系统的状态后端，状态存储在TaskManager ，检查点存储在文件系统中
         *          RocksDBStateBacked  基于RocksDBStateBacked ,状态存储在 RocksDB，检查点存储在远程文件系统中
         *      Flink 1.13 时，存在两种状态后端
         *          HashMapStateBacked          ： 同 1.12 中的 内存和文件系统，状态都存储在 TaskManager 中，可以设置保存检查点的路径
         *          EmbeddedRocksDBStateBacked ：  同 1.12 RocksDBStateBacked
         */
        // 2.6.1 设置状态存储在 TaskManager 中
        env.setStateBackend(new HashMapStateBackend());
        // 2.6.2 设置检查点存储在 JobManager 中
        //env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 3.读取 kafka中 topic_db中的业务数据-- 主流 jsonStr
        // 5.1 声明消费的主题和消费者组名
        String topic = "topic_db";
        String groupId = "dim_sink_group";
        // 5.2 创建 Kafka 消费者对象
        FlinkKafkaConsumer kfConsumer = MyKafkaUtil.getKfConsumer(topic, groupId);
        // 5.3 将 kafka 消费者作为数据源
        DataStreamSource<String> topicDbSource = env.addSource(kfConsumer);

        // TODO 4.jsonStr -- jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjStream = topicDbSource.map(
                jsonStr -> JSON.parseObject(jsonStr)
        );

        // TODO 5.对 jsonObj 进行 ETL
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjStream.filter(
                JsonObj -> {
                    try {
                        // 只获取 含有 data 字段的数据
                        JSONObject dataJsonObj = JsonObj.getJSONObject("data");
                        // 对 dataJsonStr 进行校验，校验 其中的JSON格式是否正确
                        JSONValidator.from(dataJsonObj.toJSONString()).validate();
                        // 过滤掉 bootstrap-start 和 bootstrap-complete 的数据
                        if (JsonObj.getString("type").equals("bootstrap-start") || JsonObj.getString("type").equals("bootstrap-complete")) {
                            return false;
                        }
                        return true;
                    } catch (Exception e) {
                        return false;
                    }
                }
        );
//        filterDS.print("业务流数据： ");

        /**
         *  读取 业务数据库中的数据变化，没有使用 Flink CDC 的原因？
         *      1、Flink CDC 是 Flink 1.11 时出现的，这个时候只支持 Mysql 和 PortgrSQL 两种数据库，可能存在兼容性问题
         *      2、使用 Flink CDC 采集业务数据库中变化的数据，那么 Kafka 中的 topic_db 就没有了，在 ods 层只剩下了 topic_log,这样也是不合适的，
         *          数据仓库中数据就不完整了
         *      3、FLink CDC 会采集全部表的变化，不能采集部分表的变化，不够灵活。而Maxwell 可以采集我们所需要的表变化的数据
         */
        // TODO 6.FlinkCDC 读取mysql 中的 DIM 表的配置信息-- 配置流
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall2022_config") // set captured database
                .tableList("gmall2022_config.table_process") // set captured table
                .username("root")
                .password("root")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .startupOptions(StartupOptions.initial())
                .build();

        // 使用CDC Source从MySQL读取数据
        DataStreamSource<String> mysqlDS =
                env.fromSource(
                        mySqlSource,
                        WatermarkStrategy.noWatermarks(),
                        "MysqlSource");
        // 打印 由 Flink CDC 采集到的 MySQL 中配置表数据
//        mysqlDS.print("配置流数据");

        // TODO 7.将配置流进行广播 -- 广播流
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("mapStateDescriptor", String.class, TableProcess.class);
        BroadcastStream<String> dimPropStream = mysqlDS.broadcast(mapStateDescriptor);

        // TODO 8.将广播流与 主流进行 连接 -- connect
        BroadcastConnectedStream<JSONObject, String> connectDS = filterDS.connect(dimPropStream);

        // TODO 9.连接流调用 process算子，对于广播流：在 Hbase 中创建维度表，并将维度表名通过广播状态保存
        SingleOutputStreamOperator<JSONObject> dimDS = connectDS.process(new TableProcessFunction(mapStateDescriptor));

        dimDS.print(">>>>>>");
        // TODO 10.根据广播状态中的维度表名，在主流中获取维度数据
        // TODO 11.将维度数据写入 hbase 中
        //{"name":"玻璃杯","sink_table":"dim_base_category3","id":394,"category2_id":44}
        dimDS.addSink(new DimSinkFunction());

        env.execute();
    }
}
