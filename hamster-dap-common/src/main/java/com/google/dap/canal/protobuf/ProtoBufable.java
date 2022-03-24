package com.google.dap.canal.protobuf;

/**
 * 定义protobuf序列化接口
 * 接口返回byte[]二进制字节码对象
 * 所有使用protobuf序列化的bean需要实现该接口
 */
public interface ProtoBufable {
    /**
     * 将对象转换成二进制数组
     * @return
     */
    byte[] toBytes();
}
