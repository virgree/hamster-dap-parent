package com.google.dap.realtime.etl.`trait`

import com.google.dap.canal.bean.CanalRowData
import com.google.dap.realtime.etl.util.{CanalRowDataDeserialzerSchema, GlobalConfigUtil, KafkaProps}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 * 根据数据的来源不同，可以抽象出来两个抽象类
 * 该类主要是消费canalTopic中的数据，需要将消费到的字节码数据进行反序列化（自定义反序列化）成CanalRowData
 */
abstract class MysqlBaseETL(env:StreamExecutionEnvironment) extends BaseETL[CanalRowData] {
  /**
   * 根据业务可以抽取出来kafka读取方法，因为所有的ETL都会操作kafka
   *
   * @param topic
   * @return
   */
  override def getKafkaDataStream(topic: String = GlobalConfigUtil.`input.topic.canal`): DataStream[CanalRowData] = {
    //消费的是kafka的canal数据，而binlog日志进行了protobuf的序列化，所以读取到的数据需要进行反序列化
    val canalKafkaConsumer: FlinkKafkaConsumer011[CanalRowData] = new FlinkKafkaConsumer011[CanalRowData](
      topic,
      //这个对象是自定义的反序列化对象，可以解析kafka写入到protobuf格式的数据
      new CanalRowDataDeserialzerSchema(),
      KafkaProps.getKafkaProperties()
    )

    //将消费者实例添加到环境中
    val canalDataStream: DataStream[CanalRowData] = env.addSource(canalKafkaConsumer)

    //返回消费到的数据
    canalDataStream
  }

}
