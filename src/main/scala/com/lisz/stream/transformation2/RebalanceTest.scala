package com.lisz.stream.transformation2

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
/*
增大分区数
shuffle - 随机
rebalance - 轮询
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
