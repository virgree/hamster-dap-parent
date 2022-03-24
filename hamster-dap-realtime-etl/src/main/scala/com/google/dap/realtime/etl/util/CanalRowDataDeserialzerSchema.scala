package com.google.dap.realtime.etl.util

import com.google.dap.canal.bean.CanalRowData
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema

/**
 * 自定义反序列化实现类，继承AbstractDeserializationSchema抽象类
 * 参考：SimpleStringSchema
 */
class CanalRowDataDeserialzerSchema  extends AbstractDeserializationSchema[CanalRowData]{
  /**
   * 将kafka读取到的字节码数据转换成CanalRowData对象返回
   * @param message
   * @return
   */
  override def deserialize(message: Array[Byte]): CanalRowData = {
    //需要将二进制的字节码数据转换成CanalRowData对象返回
    new CanalRowData(message);
  }
}
