package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DateFormatUtil;
import com.atguigu.gmall.realtime.bean.TrafficPageViewBean;
import com.atguigu.gmall.realtime.util.MyClickHouseUtil;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author Adam-Ma
 * @date 2022/6/3 20:11
 * @Project BigData_Review_Adam
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 * DESC : 流量域版本-渠道-地区-访客类别 粒度 页面浏览各窗口轻度聚合
 *      实现方案： Flink API
 *      当前需要统计的是
 *          维度：
 *              渠道
 *              地区
 *              版本
 *              访客类别
 *          度量值：
 *              会话数     last_page_id == null || last_page_id.length() == 0
 *              页面浏览数  1 page_log----> 1 次浏览
 *              浏览时长    1 page log ---> duration
 *
 *              独立访客数   从 独立访客事务事实表中获取，来一条数据，记一次
 *
 *              用户跳出数   从 用户跳出事务事实表中获取，来一条，记一次
 *
 * 需要开启的组件：
 *     flume , zk,  Kafka , DwdTrafficBaseLogSplit,DwdTrafficUniqueVisitorDetail,DwdTrafficUserJumpDetail,DwsTrafficVcChArIsNewPageViewWindow
 *
 * 执行流程：
 *      1、 flume 将神策大数据提供的当前项目的前端埋点日志，采集到 Kafka topic_log 主题中，作为ODS层数据
 *      2、 通过 FlinkKafkaConsumer 消费 topic_log 中的数据
 *      3、 将消费到的数据进行分流，分流到如下各主题中， 作为 流量域事务事实表
 *              流量域启动日志事务事实表        dwd_traffic_start_log
 *              流量域页面日志事务事实表        dwd_traffic_page_log
 *              流量域动作日志事务事实表        dwd_traffic_action_log
 *              流量域曝光日志事务事实表        dwd_traffic_display_log
 *              流量域错误日志事务事实表        dwd_traffic_error_log
 *      4、当前汇总需求中，对于 页面浏览数、会话数、浏览时长，都可以从 dwd_traffic_page_log 中获取到
 *      5、对于汇总需求中，独立访客数 可以通过 dwd_traffic_uniquq_visitor_detail 主题中获取到
 *      6、对于汇总需求中，用户跳出数 可以通过 dwd_traffic_user_jump_detail 主题中获取到
 *      7、将获取到的数据，都转换成相同的格式 ，当前转换为实体类：TrafficPageViewBean
 *      8、由于当前的维度是 ： 渠道 - 地区 - 版本  - 访客类别，在建立ClickHouse 表的时候，使用的引擎是 ReplacingMergeTree，
 *          并且 order(stt, edt, vc, ch, ar, is_new), 当按照 维度分组，然后开窗聚合后，将数据写入到 ClickHouse中，
 *          对于三个流来说，肯定有同一时刻（分区相同）维度相同的数据，那么保存到 ClickHouse 中就会去重到只剩下一条，
 *          所以需要将三条流进行 union 合并，然后再写入到ClickHouse 中
 *      9、设置水位线
 *      10、分组（按照维度： 渠道 - 地区 - 版本  - 访客类别）
 *      11、开窗、聚合
 *      12、写入到 ClickHouse 中
 */
public class DwsTrafficVcChArIsNewPageViewWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1.环境准备
        // 1.1 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.2 设置并行度
        env.setParallelism(4);

        // TODO 2.检查点设置（略）
        /*
        // 2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointMode.EXACTLY_ONCE);
        // 2.2 最大超时时间
        env.getCheckpointConfig().setCheckpointTimeOut(60000L);
        // 2.3 job 取消后，检查点是否保存
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 2.4 设置两个检查点之间的最小时间间隔
        env.
         */

        // TODO 3.从三个 topic 中读取数据
        // 3.1 读取的 topic 名称 和 用户组的名称
        // 3.1.1  page_log
        String pageLog      = "dwd_traffic_page_log";
        String pageLogGroup = "dwd_traffic_page_log";
        // 3.1.2  unique_visit
        String uniqueVisitor      = "dwd_traffic_unique_visitor_detail";
        String uniqueVisitorGroup = "dwd_traffic_unique_visitor_detail";
        // 3.1.3  user_jump
        String userJump           = "dwd_traffic_user_jump_detail";
        String userJumpGroup      = "dwd_traffic_user_jump_detail";
        // 3.2 生成 FlinkKafkaConsumer
        FlinkKafkaConsumer<String> pageLogConsumer       = MyKafkaUtil.getKfConsumer(pageLog, pageLogGroup);
        FlinkKafkaConsumer<String> uniqueVisitorConsumer = MyKafkaUtil.getKfConsumer(uniqueVisitor, uniqueVisitorGroup);
        FlinkKafkaConsumer<String> userJumpConsumer      = MyKafkaUtil.getKfConsumer(userJump, userJumpGroup);
        // 3.3 将消费到的数据作为数据源
        DataStreamSource<String> pageLogDS = env.addSource(pageLogConsumer);
        DataStreamSource<String> uniqueVisitorDS = env.addSource(uniqueVisitorConsumer);
        DataStreamSource<String> userJumpDS = env.addSource(userJumpConsumer);

//        pageLogDS.print(">>>>>PL<<<<<");
//        uniqueVisitorDS.print(">>>>>UV<<<<<");
//        userJumpDS.print(">>>>>UJ<<<<<");
        // TODO 4.对读取到的数据 分别进行格式转换，转换后的数据结构相同
        // 4.1 dwd_traffic_page_log 中的数据进行格式转换
        /**
         *  dwd_traffic_page_log 中的数据格式：
         *    {
         *      "common":
         *          {
         *              "ar":"110000",
         *              "uid":"626",
         *              "os":"iOS 13.3.1",
         *              "ch":"Appstore",
         *              "is_new":"1",
         *              "md":"iPhone 8",
         *              "mid":"mid_834889",
         *              "vc":"v2.1.134",
         *              "ba":"iPhone"
         *          },
         *      "page":
         *          {
         *              "page_id":"payment",
         *              "item":"25",
         *              "during_time":4314,
         *              "item_type":"sku_ids",
         *              "last_page_id":"trade"
         *         },
         *      "ts":1653403363000
         *   }
         */
        SingleOutputStreamOperator<TrafficPageViewBean> pageMapDS = pageLogDS.map(
                new RichMapFunction<String, TrafficPageViewBean>() {
                    @Override
                    public TrafficPageViewBean map(String jsonStr) throws Exception {
                        // 将 jsonStr 转换成 JSONObject
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        // 获取 common JSONObject
                        JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                        // 获取 page JSONObject
                        JSONObject pageJsonObj = jsonObj.getJSONObject("page");

                        // 页面浏览数记为 1
                        long pvCt = 1L;

                        // 定义变量表示 会话数
                        long svVt = 0L;
                        // 获取用户的 last_page_id
                        String lastPageId = pageJsonObj.getString("last_page_id");
                        if (StringUtils.isEmpty(lastPageId)) {
                            // 当 last_page_id为null 的时候
                            svVt = 1L;
                        }

                        // 获取用当前页面的访问时长
                        Long durSum = pageJsonObj.getLong("during_time");

                        // 创建对象，将当前 page_log 可获取到的信息进行返回
                        TrafficPageViewBean trafficPageViewBean = new TrafficPageViewBean(
                                "",
                                "",
                                commonJsonObj.getString("vc"),
                                commonJsonObj.getString("ch"),
                                commonJsonObj.getString("ar"),
                                commonJsonObj.getString("is_new"),
                                0L,
                                svVt,
                                pvCt,
                                durSum,
                                0L,
                                jsonObj.getLong("ts")
                        );

                        return trafficPageViewBean;
                    }
                }
        );

        // 4.2 dwd_traffic_unique_visitor_detail 中的数据格式转换
        /**
             dwd_traffic_unique_visitor_detail 中的数据格式：
                {
                    "common":
                        {
                            "ar":"110000",
                            "uid":"626",
                            "os":"iOS 13.3.1",
                            "ch":"Appstore",
                            "is_new":"1",
                            "md":"iPhone 8",
                            "mid":"mid_834889",
                            "vc":"v2.1.134",
                            "ba":"iPhone"
                       },
                    "page":
                        {
                            "page_id":"home",
                            "during_time":16398
                        },
                    "ts":1653403354000
               }
         */
        SingleOutputStreamOperator<TrafficPageViewBean> UVMapDS = uniqueVisitorDS.map(
                new RichMapFunction<String, TrafficPageViewBean>() {
                    @Override
                    public TrafficPageViewBean map(String jsonStr) throws Exception {
                        // 将 jsonStr 转换成 JSONObject
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        // 获取 common JSONObject
                        JSONObject commonJsonObj = jsonObj.getJSONObject("common");

                        TrafficPageViewBean trafficPageViewBean = new TrafficPageViewBean(
                                "",
                                "",
                                commonJsonObj.getString("vc"),
                                commonJsonObj.getString("ch"),
                                commonJsonObj.getString("ar"),
                                commonJsonObj.getString("is_new"),
                                1L,
                                0L,
                                0L,
                                0L,
                                0L,
                                jsonObj.getLong("ts")
                        );

                        return trafficPageViewBean;
                    }
                }
        );

        // 4.3 dwd_traffic_user_jump_detail 中的数据格式转换
        SingleOutputStreamOperator<TrafficPageViewBean> UJMapDS = userJumpDS.map(
                new RichMapFunction<String, TrafficPageViewBean>() {
                    @Override
                    public TrafficPageViewBean map(String jsonStr) throws Exception {
                        // 将 jsonStr 转换成 JSONObject
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        // 获取 common JSONObject
                        JSONObject commonJsonObj = jsonObj.getJSONObject("common");

                        TrafficPageViewBean trafficPageViewBean = new TrafficPageViewBean(
                                "",
                                "",
                                commonJsonObj.getString("vc"),
                                commonJsonObj.getString("ch"),
                                commonJsonObj.getString("ar"),
                                commonJsonObj.getString("is_new"),
                                0L,
                                0L,
                                0L,
                                0L,
                                1L,
                                jsonObj.getLong("ts")
                        );

                        return trafficPageViewBean;
                    }
                }
        );

        // TODO 5.将转换后的数据进行 union 操作
        DataStream<TrafficPageViewBean> unionDS = pageMapDS.union(UVMapDS, UJMapDS);

        // TODO 6.对 union 后的数据设置水位线
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithWaterMarkDS = unionDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TrafficPageViewBean>() {
                                    @Override
                                    public long extractTimestamp(TrafficPageViewBean element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                }
                        )
        );

        // TODO 7.将数据按照维度进行分组
        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> keyedDS = trafficPageViewWithWaterMarkDS.keyBy(
                new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(TrafficPageViewBean trafficPageViewBean) throws Exception {
                        return Tuple4.of(
                                trafficPageViewBean.getVc(),
                                trafficPageViewBean.getAr(),
                                trafficPageViewBean.getCh(),
                                trafficPageViewBean.getIsNew()
                        );
                    }
                }
        );

        // TODO 8.对分完组的数据进行开窗
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowDS = keyedDS.window(
                TumblingEventTimeWindows.of(Time.seconds(5))
        ).allowedLateness(Time.seconds(10));

        // TODO 9. 窗口内聚合
        SingleOutputStreamOperator<TrafficPageViewBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TrafficPageViewBean>() {
                    @Override
                    public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                        value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                        value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                        value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                        value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                        value1.setUjCt(value1.getUjCt() + value2.getUjCt());
                        return value1;
                    }
                },
                new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<TrafficPageViewBean> input, Collector<TrafficPageViewBean> out) throws Exception {
                        for (TrafficPageViewBean trafficPageViewBean : input) {
                            trafficPageViewBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                            trafficPageViewBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                            trafficPageViewBean.setTs(System.currentTimeMillis());
                            out.collect(trafficPageViewBean);
                        }
                    }
                }
        );

        reduceDS.print(">>>>>>");

        // TODO 9.将聚合的结果写入到 ClickHouse中
        reduceDS.addSink(
                MyClickHouseUtil.<TrafficPageViewBean>getJdbcSink("insert into dws_traffic_vc_ch_ar_is_new_page_view_window values(?,?,?,?,?,?,?,?,?,?,?,?)")
        );

        env.execute();
    }
}
