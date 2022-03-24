package com.google.dap.realtime.etl.util

import com.typesafe.config.ConfigFactory

/**
 * 编写读取配置文件的工具类
 */
object GlobalConfigUtil {
  //默认在resources目录下找application名字的配置文件
  private val config = ConfigFactory.load()

  /**
   * 读取配置文件的配置项信息
   */
  val `bootstrap.servers` = config.getString("bootstrap.servers")
  val `zookeeper.connect` = config.getString("zookeeper.connect")
  val `input.topic.canal` = config.getString("input.topic.canal")
  val `group.id` = config.getString("group.id")
  val `enable.auto.commit` = config.getString("enable.auto.commit")
  val `auto.commit.interval.ms` = config.getString("auto.commit.interval.ms")
  val `auto.offset.reset` = config.getString("auto.offset.reset")
  val `key.serializer` = config.getString("key.serializer")
  val `key.deserializer` = config.getString("key.deserializer")

  val `redis.server.ip` = config.getString("redis.server.ip")
  val `redis.server.port`: String = config.getString("redis.server.port")
  val `mysql.server.ip` = config.getString("mysql.server.ip")
  val `mysql.server.port` = config.getString("mysql.server.port")
  val `mysql.server.database` = config.getString("mysql.server.database")
  val `mysql.server.username` = config.getString("mysql.server.username")
  val `mysql.server.password` = config.getString("mysql.server.password")

  def main(args: Array[String]): Unit = {
    println(`input.topic.canal`)
  }
}
