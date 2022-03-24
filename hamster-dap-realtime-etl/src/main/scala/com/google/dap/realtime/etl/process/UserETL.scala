package com.google.dap.realtime.etl.process

import com.google.dap.canal.bean.CanalRowData
import com.google.dap.realtime.etl.`trait`.MysqlBaseETL
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * 用户实时ETL操作，根据业务维度表进行拉伸
 * @param env
 */
case class UserETL(env: StreamExecutionEnvironment)  extends  MysqlBaseETL(env){
  /**
   * 实现process方法
   */
  override def process(): Unit = {

    // 1. 过滤出来 user_user 表的日志，并进行转换
    val userCanalDS: DataStream[CanalRowData] = getKafkaDataStream().filter(_.getTableName == "user_user")

    // 2. 使用同步IO方式请求Redis拉取维度数据

    //3：将商品数据转换成json字符串

    //4：将商品数据写入到kafka集群

  }
}
