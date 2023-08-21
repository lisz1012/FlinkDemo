package com.lisz.stream.transformation2

import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

// 这个演示要打包后上传到集群运行, 先开启 nc 再提交启动 Flink application
// flink run -c com.lisz.stream.transformation2.CheckpointTest2 -d ~./FlinkDemo-1.0-SNAPSHOT.jar
// 在 WebUI 上 Cancel 之后再次提交:
// flink run -c com.lisz.stream.transformation2.CheckpointTest2 -d -s hdfs://hadoop-01:9000/flink/checkpoint/JOB_ID/chk-xxx  ~./FlinkDemo-1.0-SNAPSHOT.jar
object CheckpointTest2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置状态后端, 也就是 state 存到哪里去
    env.setStateBackend(new FsStateBackend("hdfs://hadoop-01:9000/flink/checkpoint", true))
    // 每隔 1s 网数据源中插图一个栅栏 barrier
    env.enableCheckpointing(1000)
    // 容许 checkpoint 失败的次数
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(2)
    // 超时时间
    env.getCheckpointConfig.setCheckpointTimeout(5 * 60 * 1000)
    // 设置模式/语义. 默认就是EXACTLY_ONCE
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 设置相邻两个checkpoint 任务之间的最小时间间隔. 单位是 ms, 最少 10 毫秒, 否则报错.
    // 当可以开始下一个checkpoint 任务的时候, 先不开始, 等待设置的时间之后再开始
    // 防止出发太密集的 Flink Checkpoint 导致消耗过多 Flink 资源
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(600)
    // 设置 checkpoint 最大并行个数, 1 是串行执行, 上面的 minPauseBetweenCheckpoints 设置
    // 了, 这个并行度默认就是 1, 所以其实只设置上面的minPauseBetweenCheckpoints就行.
    // 如果设置了 > 1 的并行度, 则上面的 minPauseBetweenCheckpoints  就不要设了
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // Flink 取消之后, checkpoint 数据是否删除?
    // RETAIN_ON_CANCELLATION: 保留
    // DELETE_ON_CANCELLATION: 删除
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    val stream = env.socketTextStream("hadoop-01", 8888);
    stream.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .reduce((v1: (String, Int), v2: (String, Int)) => {
        (v1._1, v1._2 + v2._2)
      }).print
    env.execute
  }
}
