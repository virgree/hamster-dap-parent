package com.google.dap.canal.util;

import java.util.Properties;

/**
 * 编写工具类读取config.properties配置文件信息
 */
public class ConfigUtil {
    //定义个properties对象
    private static Properties properties;

    //定义一个静态的代码块，该代码只被执行一次
    static{
        try {
            properties = new Properties();
            properties.load(ConfigUtil.class.getClassLoader().getResourceAsStream("config.properties"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String canalServerIp() {
        return properties.getProperty("canal.server.ip");
    }

    public static int canalServerPort() {
        return Integer.parseInt(properties.getProperty("canal.server.port"));
    }

    public static String canalServerDestination() {
        return properties.getProperty("canal.server.destination");
    }

    public static String canalServerUsername() {
        return properties.getProperty("canal.server.username");
    }

    public static String canalServerPassword() {
        return properties.getProperty("canal.server.password");
    }

    public static String canalSubscribeFilter() {
        return properties.getProperty("canal.subscribe.filter");
    }

    public static String zookeeperServerIp() {
        return properties.getProperty("zookeeper.server.ip");
    }

    public static String kafkaBootstrap_servers_config() {
        return properties.getProperty("kafka.bootstrap_servers_config");
    }

    public static String kafkaBatch_size_config() {
        return properties.getProperty("kafka.batch_size_config");
    }

    public static String kafkaAcks() {
        return properties.getProperty("kafka.acks");
    }

    public static String kafkaRetries() {
        return properties.getProperty("kafka.retries");
    }

    public static String kafkaBatch() {
        return properties.getProperty("kafka.batch");
    }

    public static String kafkaClient_id_config() {
        return properties.getProperty("kafka.client_id_config");
    }

    public static String kafkaKey_serializer_class_config() {
        return properties.getProperty("kafka.key_serializer_class_config");
    }

    public static String kafkaValue_serializer_class_config() {
        return properties.getProperty("kafka.value_serializer_class_config");
    }

    public static String kafkaTopic() {
        return properties.getProperty("kafka.topic");
    }

    public static void main(String[] args) {
        System.out.println(kafkaTopic());
    }
}
