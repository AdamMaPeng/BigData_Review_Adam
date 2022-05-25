package com.atguigu.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

/**
 * @author Adam-Ma
 * @date 2022/5/23 21:14
 * @Project BigData_Review_Adam
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 * DESC : 将 topic_log 中的数据分到分类日志各自的topic 中
 *  需要开启的组件：
 *      SpringBoot Jar包，flume, ZK， Kafka， Hadoop
 *
 *  实现流程     ：
 *      1、通过 applog 下的 springBoot jar包，运行 jar生成 日志数据
 *      2、将生成的日志数据 通过 flume 采集到Kafka 的 topic_log 主题中
 *      3、Flink 程序读取 topic_log 中的数据
 *      4、对数据格式进行转换 ： jsonStr -> jsonObj , 然后将转换报错的脏数据，写入到侧输出流中
 *      5、将脏数据的流数据，写入到 Kafka 脏数据 的 dirty_log 主题中
 *      6、非脏数据中，对新老访客的状态进行修复-- 通过 Flink 状态编程
 *      7、将正常数据进行分流，按照流量域各业务过程划分为如下不同的业务过程
 *              错误日志： dwd_traffic_error_log
 *              启动日志： dwd_traffic_start_log
 *              页面日志： dwd_traffic_page_log
 *              曝光日志： dwd_traffic_display_log
 *              动作日志： dwd_traffic_action_log
 *      8、将分好的流写入 Kafka 对应的 topic 下 ，作为流量域事实表
 *
 */
public class DwdTrafficBaseLogSplit {
    public static void main(String[] args) throws Exception {
        // TODO 1.环境准备
        // 1.1 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.2 设置全局并行度
        env.setParallelism(4);

        // TODO 2.检查点设置
        // 2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        // 2.2 设置超时时间 ： 若设置检查点时，网络不畅，一直设置不成功，最大的超时设置时间，如果超时则抛异常
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        // 2.3 两个检查点之间的间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        // 2.4 Job 取消时，检查点是否保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 2.5 重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.milliseconds(50L)));
        // 2.6 状态后端
        env.setStateBackend(new HashMapStateBackend());
        // 2.7 将检查点保存在 hdfs 中
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck/applog");
        // 2.7 将操作HDFS用户改为 atguigu
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 3.读取 Kafka 中 topic_log中的数据
        // topic 名称
        String topic_log = "topic_log";
        // 消费者组名称
        String groupId = "dwd_traffic_baseL_log_group";
        // 3.1 从 topic_log 中消费数据
        FlinkKafkaConsumer<String> kfConsumer = MyKafkaUtil.getKfConsumer(topic_log, groupId);
        DataStreamSource<String> kafkaStrDS = env.addSource(kfConsumer);

        kafkaStrDS.print("topic_log : ");

        // TODO 4. 将读取到的 topic_log 数据转换格式： jsonStr-> JsonObject,并进行简单的ETL，将脏数据写入 dirty_Log 主题中
        // 4.1 定义dirty_log 侧输出流标签
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag"){};
        // 4.2 jsonStr -> JsonObject, 转换报错的写入侧输出流中
        SingleOutputStreamOperator<JSONObject> cleanedDS = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    out.collect(jsonObj);
                } catch (Exception e) {
                    //如果将 JsonStr -> JsonObject 失败，则为脏数据，写入到侧输出流中
                    ctx.output(dirtyTag, jsonStr);
                }
            }
        });
//        cleanedDS.print("非脏数据 ： ");
        // 4.3 获取侧输出流
        DataStream<String> dirtyLogDS = cleanedDS.getSideOutput(dirtyTag);
//        dirtyLogDS.print("脏数据 ：");
        // 4.4 将侧输出流中的数据写入到 Kafka的 dirtyLog  主题中
        String dirtyTopic = "topic_dirtyLog";
        dirtyLogDS.addSink(MyKafkaUtil.getKfProducer(dirtyTopic));

        // TODO 5.新老访客状态修复 --- Flink 状态编程
        // 5.1 根据用户进行分组
        KeyedStream<JSONObject, String> userKS = cleanedDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        // 5.2 调用 map 方法，通过 valueState 保存用户历史的访问时间
        /**
         *  用户状态修复逻辑
         * is_new = 1 :
         *      1) ts -> currentDate, 若 stateDate = null ，则为新用户，将 stateDate = currentDate
         *      2) ts -> currentDate, 若 stateDate !=null , stateDate == currentDate ，则为新用户，不做操作
         *      3) ts -> currentDate, 若 stateDate !=null , stateDate ！= currentDate ，则为老用户，将 is_new = 0
         *  is_new = 0 :
         *      1) ts -> currentDate, 若 date  = null, 则 stateDate = currentDate - 1
         *      2）ts -> currentDate, 若 date != null, 不做操作
         */
        SingleOutputStreamOperator<JSONObject> repairUserStateDS = userKS.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    // 5.2.1 声明 ValueDate ，用户保存用户访问页面的日期状态
                    private ValueState<String> stateDate = null;
                    // 5.2.6 声明 DateTimeFormatter 对象
                    private SimpleDateFormat sdf = null;

                    /**
                     *  5.2.2 在生命周期 open 方法中，对 stateDate 进行初始化
                     * @param parameters
                     * @throws Exception
                     */
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        stateDate = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastVisitStateDate", String.class));
                        // 当算子执行的时候，创建 dtf 对象
                        sdf = new SimpleDateFormat("yyyy-MM-dd");
                    }

                    @Override
                    public JSONObject map(JSONObject jsonStr) throws Exception {
                        // 5.2.3 先获取最后一次状态中的值
                        String lastVisitDate =  stateDate.value();
                        // 5.2.4 从 jsonStr 中获取 is_new 字段
                        String isNew = jsonStr.getJSONObject("common").getString("is_new");
                        // 5.2.5 从 jsonStr 中获取 ts 时间戳，然后转换成 yyyy-MM-dd 的格式
                        Long ts = jsonStr.getLong("ts");
                        String currentDate = sdf.format(ts);

                        // 5.2.7 根据相应的规则进行判断
                        if ("1".equals(isNew)) {
                            if (lastVisitDate == null || lastVisitDate.length() == 0) {
                                stateDate.update(currentDate);
                            } else {
                                if (!lastVisitDate.equals(currentDate)) {
                                    jsonStr.getJSONObject("common").put("is_new", "0");
                                }
                            }
                        } else {
                            if (lastVisitDate == null || lastVisitDate.length() == 0) {
                                String yesterday = sdf.format(ts - 1000 * 60 * 60 * 24);
                                stateDate.update(yesterday);
                            }
                        }
                        return jsonStr;
                    }
                }
        );
        // 打印已经修复好的用户状态流的数据
//        repairUserStateDS.print("用户状态已修复： ");


        env.execute();
    }
}
