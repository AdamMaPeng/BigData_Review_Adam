package com.atguigu.gmall.realtime.util;

import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * @author Adam-Ma
 * @date 2022/5/22 12:18
 * @Project BigData_Review_Adam
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 * DESC : Kafka 相关的工具类
 */
public class MyKafkaUtil {
    /**
     *  获取 Kafka 消费者
     * @param topic     主题名
     * @param groupId   消费者组名称
     * @return
     */
    public static FlinkKafkaConsumer<String> getKfConsumer(String topic , String  groupId){
        // Kafka 相关的配置
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, GmallConfig.KF_BOOTSTRAP_SERVER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // 采用 SimpleStringSchema 的反序列化方式，对于 null 的值，无法反序列化，会报错
//        new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), props);

        FlinkKafkaConsumer<String> stringFlinkKafkaConsumer = new FlinkKafkaConsumer<>(topic, new KafkaDeserializationSchema<String>() {
            @Override
            public boolean isEndOfStream(String s) {
                return false;
            }

            @Override
            public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                if (consumerRecord != null && consumerRecord.value() != null) {
                    return new String(consumerRecord.value());
                }
                return null;
            }

            @Override
            public TypeInformation<String> getProducedType() {
                return TypeInformation.of(String.class);
            }
        }, props);
        return stringFlinkKafkaConsumer;
    }

    /**
     * 获取 Kafka 生产者
     * @param topicName 需要发送数据到 的 topic 名称
     * @return
     */
    public static FlinkKafkaProducer<String> getKfProducer(String topicName){
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, GmallConfig.KF_BOOTSTRAP_SERVER);
        // FlinkKafkaProducer继承两阶段提交，涉及到事务，需要设置事务提交延时，默认最大15min
        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 1000 + "");

        FlinkKafkaProducer<String> stringFlinkKafkaProducer = new FlinkKafkaProducer<String>("default_topic", new KafkaSerializationSchema<String>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(String jsonStr, @Nullable Long timestamp) {
                return new ProducerRecord<byte[], byte[]>(topicName, jsonStr.getBytes());
            }
        }, props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        return stringFlinkKafkaProducer;
    }

    /**
     *  FlinkSQL中 消费 kafka 数据的 连接配置
     * @param topidName  主题名称
     * @param groupId    消费者组名称
     * @return
     */
    public static String getKafkaDDL(String topidName, String groupId){
        return "WITH ( 'connector' = 'kafka',\n" +
                        "  'topic' = '" + topidName + "',\n" +
                        "  'properties.bootstrap.servers' = ' "+ GmallConfig.KF_BOOTSTRAP_SERVER +"',\n" +
                        "  'properties.group.id' = '"+ groupId +"',\n" +
                        "  'scan.startup.mode' = 'group-offsets',\n" +
                        "  'format' = 'json'\n" +
                    ")";
    }

    /**
     *  FlinkSQL 中向 Kafka 中发送数据的，建表的配置
     * @param topicName 主题名称
     * @return
     */
    public static String getUpsertKafkaDDL(String topicName){
        return "WITH (  'connector' = 'upsert-kafka',\n" +
                        "  'topic' = '"+ topicName +"',\n" +
                        "  'properties.bootstrap.servers' = '"+ GmallConfig.KF_BOOTSTRAP_SERVER +"',\n" +
                        "  'key.format' = 'json',\n" +
                        "  'value.format' = 'json'\n" +
                        ")";
    }

    /**
     *  FLink SQL 中获取 topic_db 中的数据，创建为临时表
     */
    public static String getTopicDbTable(String groupId){
        return "create table topic_db (\n" +
                "  `database` string,\n" +
                "  `table`    string,\n" +
                "  `type`     STRING,\n" +
                "  `ts`       string,\n" +
                "  `data`     map<string, string>,\n" +
                "  `old`      map<string, string>,\n" +
                "  `proc_time` as proctime()\n" +
                ") " + MyKafkaUtil.getKafkaDDL("topic_db",groupId);
    }
}
