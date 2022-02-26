package com.lisz.stream.transformation2

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/*
  上有4个分区，下游2个分区，会两个为一组进入下游分区，尽量形成"窄依赖"
  减少分区数，防止大量的网络传输，相当于spark里的coalesce
 */
object Rescale {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.generateSequence(1, 10).setParallelism(2)
    stream.writeAsText("./data/stream3").setParallelism(2)
    stream.rescale.writeAsText("./data/stream4").setParallelism(4) // 2 -> 4 也是，尽量互相不掺合，而且平均
    env.execute
  }

}
