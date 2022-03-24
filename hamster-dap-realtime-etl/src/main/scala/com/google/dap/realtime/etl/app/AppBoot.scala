package com.google.dap.realtime.etl.app

import com.google.dap.realtime.etl.process.UserETL
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * ETL业务实现
 */
object AppBoot {

  /**
   * 入口函数
   * @param args
   */
  def main(args: Array[String]): Unit = {
    /**
     * 实现步骤：
     * 1：初始化flink的流式运行环境
     * 2：设置flink的并行度为1，测试环境设置为1，生产环境需要注意，尽可能使用client递交作业的时候指定并行度
     * 3：开启flink的checkpoint
     * 4：接入kafka的数据源，消费kafka的数据
     * 5：实现所有ETL业务
     * 6：执行任务
     */
    //1：初始化flink的流式运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //设置flink的并行度为1，测试环境设置为1，生产环境需要注意，尽可能使用client递交作业的时候指定并行度，通过参数指定。
    env.setParallelism(1)

    //开启checkpoint，设置checkpoint的运行周期，每隔5秒钟进行一次checkpoint
    env.enableCheckpointing(5000)
    //当作业被cancel的时候，保留以前的checkpoint，避免数据的丢失
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //设置同一个时间只能有一个检查点，检查点的操作是否可以并行，1不能并行
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // checkpoint的HDFS保存位置
    env.setStateBackend(new FsStateBackend("hdfs://node1:8020/flink/checkpoint/"))
    // 配置两次checkpoint的最小时间间隔
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000)
    // 配置checkpoint的超时时长
    env.getCheckpointConfig.setCheckpointTimeout(60000)

    //指定重启策略，默认的是不停的重启
    //程序出现异常的时候，会进行重启，重启五次，每次延迟5秒钟，如果超过了五次，程序退出
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 5000))

    //todo 实现所有ETL业务
    val userProcess: UserETL = new UserETL(env)
    userProcess.process()

    env.execute()
  }
}
