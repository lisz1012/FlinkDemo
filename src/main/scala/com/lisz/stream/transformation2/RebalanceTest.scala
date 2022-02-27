package com.lisz.stream.transformation2

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
/*
增大分区数
shuffle - 随机
rebalance - 轮询
两者在数据量比较大的时候都能够平均分配
 */
object RebalanceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.generateSequence(1, 100).setParallelism(3)
    println(stream.getParallelism)
    stream.rebalance.print  // 使得每个分区得到的数据都更平均
    env.execute
  }
}
