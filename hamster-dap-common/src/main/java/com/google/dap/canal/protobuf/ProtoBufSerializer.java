package com.google.dap.canal.protobuf;


import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * 实现kakfa-value的自定义序列化对象
 * 传递的泛型必须是实现ProtoBufable接口的实现类，才可以被序列化成功
 */
public class ProtoBufSerializer implements Serializer<ProtoBufable> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, ProtoBufable data) {
        return data.toBytes();
    }

    @Override
    public void close() {

    }
}
