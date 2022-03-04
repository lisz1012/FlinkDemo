package com.lisz.stream.transformation2

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/*
  上下游的分区一对一，窄依赖，他不与许下游的分区数大于上游：map、flatMap都是Forward分区策略，向前分发
  Forward partitioning does not allow change of parallelism. 不允许改变并行度
 */
object ForwardTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.generateSequence(1, 10).setParallelism(2)
    stream.writeAsText("./data/stream7").setParallelism(2)
    stream.forward.writeAsText("./data/stream8").setParallelism(2)
    env.execute
  }
}
