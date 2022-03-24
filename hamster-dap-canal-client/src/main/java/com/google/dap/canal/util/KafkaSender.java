package com.google.dap.canal.util;

import com.google.dap.canal.bean.CanalRowData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * kafka生产者工具类
 */
public class KafkaSender {
    //定义properties对象，封装kafka的相关参数
    private Properties kafkaProps = new Properties();
    //定义生产者对象，value使用的是自定义序列化的方式,该序列化方式要求传递的是一个Protobufable的子类
    private KafkaProducer<String, CanalRowData> kafkaProducer;

    //初始化kafka的生产者对象
    public KafkaSender(){
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigUtil.kafkaBootstrap_servers_config());
        kafkaProps.put(ProducerConfig.BATCH_SIZE_CONFIG, ConfigUtil.kafkaBatch_size_config());
        kafkaProps.put(ProducerConfig.ACKS_CONFIG, ConfigUtil.kafkaAcks());
        kafkaProps.put(ProducerConfig.RETRIES_CONFIG, ConfigUtil.kafkaRetries());
        kafkaProps.put(ProducerConfig.CLIENT_ID_CONFIG, ConfigUtil.kafkaClient_id_config());
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ConfigUtil.kafkaKey_serializer_class_config());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ConfigUtil.kafkaValue_serializer_class_config());

        //实例化生产者对象
        kafkaProducer = new KafkaProducer<String, CanalRowData>(kafkaProps);
    }

    /**
     * 传递参数，将数据写入到kafka集群
     * @param rowData
     */
    public void send(CanalRowData rowData){
        kafkaProducer.send(new ProducerRecord<>(ConfigUtil.kafkaTopic(), rowData));
    }
}
