package com.atguigu.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DateFormatUtil;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Adam-Ma
 * @date 2022/5/25 17:00
 * @Project BigData_Review_Adam
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 * DESC : 流量域事实表独立访客实现
 * 需要开启的组件：
 *      flume 、zk、 kafka 、HDFS 、DwdTrafficBaseLogSplit
 * 实现流程：
 *      1、从 topic_log 中消费数据
 *      2、经过 DwdTrafficBaseLogSplit 将topic_log 中的数据进行分流，分到不同的 流量域事实表中
 *      3、对于独立访客，需要从 dwd_traffic_page_log 中消费数据
 *      4、对于独立访客来说，就是每天某个用户只有一条记录。先过滤掉 last_page_id ！= null，将所有单次访问跳转的数据都过滤掉
 *      5、根据 mid 进行 keyBy 分组，每个用户单独处理
 *      6、维护一个状态，保存最后一次登录日期。
 *      7、由于是每日都统计独立访客，所以需要给状态一个过期时间 TTL(Time to Live)
 *      8、利用状态过滤掉当天已经访问过的 Mid
 *      9、提取字段 并写入 Kafka 独立访客主题
 */
public class DwdTrafficUniqueVisitorDetail {
    public static void main(String[] args) throws Exception {
        // TODO 1.环境准备
        // 1.1 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.2 设置并行度
        env.setParallelism(4);
        // TODO 2.设置检查点
        // 2.1  开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        // 2.2  设置超时时长
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        // 2.3  设置两检查点之间的最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        // 2.4  Job取消后，检查点是否保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 2.5  重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.milliseconds(3L)));
        // 2.6  设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        // 2.7  设置检查点保存位置
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck/dwd/DwdTrafficUniqueVisitorDetail");
        // 2.8  操作HDFS 的用户
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 3.读取 Kafka中 dwd_traffic_page_log 数据
        // 3.1  读取的主题名
        String pageLogTopic = "dwd_traffic_page_log";
        // 3.2  消费者名称
        String groupId = "dwd_traffic_UV_detail_group";
        // 3.3  调用 MyKafkaUtil 工具类进行消费
        DataStreamSource<String> pageLogDStream = env.addSource(MyKafkaUtil.getKfConsumer(pageLogTopic, groupId));

        // TODO 4.过滤我们想要的 last_page_id == null 的数据
        SingleOutputStreamOperator<JSONObject> filterDS = pageLogDStream.map(jsonStr -> JSON.parseObject(jsonStr))
                .filter(jsonObj -> !jsonObj.getJSONObject("page").containsKey("last_page_id"));
        filterDS.print("filter : ");

        // TODO 5.根据用户进行 keyBy
        KeyedStream<JSONObject, String> keyedStream = filterDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
//        keyedStream.print("keyedStream : ");

        // TODO 6.利用状态过滤掉今天已经访问的访客
        SingleOutputStreamOperator<String> uvDS = keyedStream.process(new KeyedProcessFunction<String, JSONObject, String>() {
            // 6.1 定义一个状态
            private ValueState<String> lastVisitState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> lastVisitDateDescript = new ValueStateDescriptor<>("lastVisitDate", String.class);
                // 6.2 通过状态描述器设置 状态的 TTL
                lastVisitDateDescript.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                lastVisitState = getRuntimeContext().getState(lastVisitDateDescript);
            }

            @Override
            public void processElement(JSONObject jsonObj, Context ctx, Collector<String> out) throws Exception {
                // 6.2 获取当前用户当前 的访问时间
                Long ts = jsonObj.getLong("ts");
                String currentDate = DateFormatUtil.toDate(ts);
                String lastVisitDate = lastVisitState.value();
                if (lastVisitDate == null || lastVisitDate.length() == 0 || !currentDate.equals(lastVisitDate)) {
                    lastVisitState.update(currentDate);
                    out.collect(jsonObj.toJSONString());
                }
            }
        });

        uvDS.print("独立访客 ： ");

        // TODO 7.将处理过后的数据写入到 Kafka 对应分区中
        // 7.1 topic 的名称
        String topicName = "dwd_traffic_unique_visitor_detail";
        // 7.2 消费者组
        String uvGroupId = "dwd_traffic_unique_visitor_detail_group";
        // 7.3 通过 addSink() ,将 数据写到对应的 Kafka 中
        uvDS.addSink(MyKafkaUtil.getKfProducer(topicName));

        env.execute();
    }
}
