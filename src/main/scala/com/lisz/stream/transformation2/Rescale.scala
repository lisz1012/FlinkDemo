package com.lisz.stream.transformation2

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/*
  上游有4个分区，下游2个分区，会两个为一组进入下游分区，尽量形成"窄依赖"
  减少分区数，防止大量的网络传输，相当于spark里的coalesce
  上游有2个分区，下游有4个，则上游的一个分区只会分发数据给某两个下游分区，避免全网分发，尽可能避免网络传输数据，尽可能分发给当前节点上执行的subTask（分区）
  降低分区数，并行度并不是越高执行的越快，超过一定阈值的时候，线程启动和调度的时间可能会大于处理数据的耗时
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
