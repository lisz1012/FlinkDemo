package com.lisz.stream.transformation2

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
/*
shuffle增大分区数。此时最好用shuffle或者rebalance。而rescale分发得不会特别均匀
shuffle - 随机
rebalance - 轮询
 */
object PartitionText {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.generateSequence(1, 10).setParallelism(1)
    println(stream.getParallelism)
    stream.shuffle.print  // stream.shuffle之后得到一个新的data stream 默认有16个分区，因为当前mac有16个核，然后把records随机分配到各个分区
    env.execute
  }
}
