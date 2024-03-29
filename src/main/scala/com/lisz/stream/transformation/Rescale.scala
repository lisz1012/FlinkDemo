package com.lisz.stream.transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * 减少分区，防止大量的网络传输。但是这个分区方法分的不一定均匀，上游数据倾斜的话，下游也会倾斜
 */
object Rescale {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.generateSequence(1, 10).setParallelism(2)
    stream.writeAsText("./data/stream1").setParallelism(2) // stream1目录下有两个文件
    stream.rescale.writeAsText("./data/stream2").setParallelism(4) // scale以分区为单位分发，反过来4->2也是。
    env.execute
  }
}
