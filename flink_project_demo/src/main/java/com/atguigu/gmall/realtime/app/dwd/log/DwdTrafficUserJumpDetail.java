package com.atguigu.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @author Adam-Ma
 * @date 2022/5/25 22:37
 * @Project BigData_Review_Adam
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 * DESC : 流量域用户跳出事务事实表
 * 需要开启的组件：
 *      flume、zk、kafka 、HDFS、DwdTrafficBaseLogSplit（page_log 数据）
 * 执行流程：
 *      1、flume 采集前端埋点用户行为日志
 *      2、将采集到的日志数据发送到 Kafka 中的 topic_log 主题中
 *      3、Flink 程序读取 topic_log 数据，对用户状态修复后，将日志数据按照不同业务过程进行分流
 *      4、将分流后的数据写入各自业务过程的 Kafka topic 中
 *      5、对于 用户跳出业务过程来说，需要读取 dwd_traffic_page_log 中的数据
 *      6、用户跳出：
 *          某个用户的第一次登录为 page 首页：last_page_id == null ，若后续的日志中 last_page_id != null ，则证明此次日志为跳转日志，而不是跳出日志
 *                                                               若后续的日志中 last_page_id == null,   则证明此次日志为跳出日志
 *                                                               若过了超时时间，假设为 30 min， 还没有收到下一条日志，则证明也当前日志也为跳转日志
           由于用到了第一个事件 ，同时还要兼顾第二个事件，这种多个事件处理问题的场景，就需要使用到 Flink CEP (Complex Event Processing) 复杂事务处理了
            还要添加两个事务之间的超时时间。
        7、将通过 Flink CEP 处理得到的符合要求的跳出日志，写入到 Kafka 的 dwd_traffic_user_jump_detail topic 中

    Flink CEP 操作的流程：
        制定事件处理的模板
        将模板应用到流上
        从流中提取数据
 */
public class DwdTrafficUserJumpDetail {
    public static void main(String[] args) throws Exception {
        // TODO 1.环境准备
        // 1.1  流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.2  设置并行度
        env.setParallelism(4);
        // TODO 2.检查点相关设置
        // 2.1  开启检查点
//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        // 2.2  设置超时时长
//        env.getCheckpointConfig().setCheckpointTimeout(60000L);
//        // 2.3  设置两检查点之间的最小间隔
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
//        // 2.4  Job取消后，检查点是否保留
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        // 2.5  重启策略
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, org.apache.flink.api.common.time.Time.days(30), org.apache.flink.api.common.time.Time.milliseconds(3L)));
//        // 2.6  设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
//        // 2.7  设置检查点保存位置
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck/dwd/DwdTrafficUserJumpDetail");
//        // 2.8  操作HDFS 的用户
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 3.从 kafka 中 dwd_traffic_page_log中读取数据
          /*DataStream<String> pageLogDS = env
            .fromElements(
                "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
                "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                    "\"home\"},\"ts\":15000} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                    "\"detail\"},\"ts\":30000} "
            );*/

        // 3.1 topic名称
        String topicName = "dwd_traffic_page_log";
        // 3.2 消费者组名称
        String groupId = "dwd_traffic_page_log_group";
        DataStreamSource<String> pageLogDS = env.addSource(MyKafkaUtil.getKfConsumer(topicName, groupId));

        // TODO 4.将读取到的数据设置水位线
        // 4.1  将读取到的数据进行格式转换
        SingleOutputStreamOperator<JSONObject> wmDS = pageLogDS.map(jsonStr -> JSON.parseObject(jsonStr))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        return jsonObj.getLong("ts");
                                    }
                                }
                        ));

        // TODO 5.将数据通过 mid 进行分组
        KeyedStream<JSONObject, String> keyedStream = wmDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        keyedStream.print("keyedStream : ");

        // TODO 6.通过 Flink CEP 进行处理两个连续的 页面数据
        // 6.1  制定 CEP 模板
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first").where(
                new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) {
                        String firstPage = jsonObj.getJSONObject("page").getString("last_page_id");
                        return (firstPage == null || firstPage.length() == 0);
                    }
                }
        ).next("second").where(
                new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) {
                        String firstPage = jsonObj.getJSONObject("page").getString("last_page_id");
                        return (firstPage == null || firstPage.length() == 0);
                    }
                }
        ).within(Time.seconds(10));
        // 6.2  将模板应用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        // TODO 7.提取匹配的事件
        // 7.1  由于超时数据会被放入侧输出流中，所以需要定义一个测输出流标签
        OutputTag<JSONObject> timeOutData = new OutputTag<JSONObject>("timeOutData"){};
        // 7.2  通过cep 的flatSelect 选择数据
        SingleOutputStreamOperator<JSONObject> cepResultDS = patternStream.flatSelect(
                timeOutData,
                new PatternFlatTimeoutFunction<JSONObject, JSONObject>() {
                    // 处理迟到事件
                    @Override
                    public void timeout(Map<String, List<JSONObject>> map, long l, Collector<JSONObject> collector) throws Exception {
                        // 迟到的事件也就是在 第一个事件当中，第一个事件中只有一个事件，所以直接通过 collector 写入到测输出流中即可
                        for (JSONObject jsonObject : map.get("first")) {
                            collector.collect(jsonObject);
                        }
                    }
                },
                new PatternFlatSelectFunction<JSONObject, JSONObject>() {
                    // 处理需要从模板中选择匹配到的数据
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> map, Collector<JSONObject> collector) throws Exception {
                        // 同时满足模板的事件， 第一个事件就是我们需要的 user_Jump的数据，所以直接将第一个事件写入到主流当中即可
                        for (JSONObject jsonObject : map.get("first")) {
                            collector.collect(jsonObject);
                        }
                    }
                }
        );

        // TODO 8.将超时未收到数据的第一个事件提取出来
        DataStream<JSONObject> lateDateDS = cepResultDS.getSideOutput(timeOutData);

        // TODO 9.将匹配的事件与 超时未收到数据的第一个事件 union 起来
        DataStream<JSONObject> unionDS = cepResultDS.union(lateDateDS);
        unionDS.print(">>>>>>>");

        // TODO 10.将最终 union 的结果写入到 Kafka 的 dwd_traffic_user_jump_detail topic 中
        // 10.1 将 user_jump 数据 写入 topic 的名称
        String resultTopic = "dwd_traffic_user_jump_detail";
        // 10.2 将 unionDS 进行格式转换
        unionDS.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getKfProducer(resultTopic));

        env.execute();
    }
}
