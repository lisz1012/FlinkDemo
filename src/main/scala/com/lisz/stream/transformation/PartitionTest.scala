package com.lisz.stream.transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object PartitionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.generateSequence(1, 10).setParallelism(1)
    println(stream.getParallelism)
    stream.shuffle.print // shuffle之后默认有16个分区，因为有16个核，shuffle的话random.nextInt(numberOfChannels)一下，随机分发
    env.execute
  }
}
