package com.google.dap.realtime.etl.`trait`

import com.google.dap.realtime.etl.util.KafkaProps
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper

/**
 * 定义特质，抽取所有etl操作公共的方法
 */
trait BaseETL[T] {
  /**
   * 构建kafka的生产者对象
   * @param topic
   */
  def kafkaProducer(topic:String)= {
    //将所有的ETL后的数据写入到kafka集群中，写入的时候类sing都是json格式
    new FlinkKafkaProducer011[String](
      topic,
      //这种方式常用于读取kafka的数据，在写入到kafka集群的场景
      new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),
      KafkaProps.getKafkaProperties()
    )
  }

  /**
   * 根据业务可以抽取出来kafka读取方法，因为所有的ETL都会操作kafka
   * @param topic
   * @return
   */
  def getKafkaDataStream(topic:String) :DataStream[T]

  /**
   * 根据业务抽取出来process方法，因为所有的ETL都有操作方法
   */
  def process()
}
