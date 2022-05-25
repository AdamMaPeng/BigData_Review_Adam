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
}
