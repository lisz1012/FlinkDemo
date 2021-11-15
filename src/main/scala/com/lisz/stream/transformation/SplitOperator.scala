package com.lisz.stream.transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

// split算子可以根据某些条件拆分数据流
object SplitOperator {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.generateSequence(1, 100)
    // 偶数分到一个流first 奇数分到另外一个流second
    val splitStream = stream.split(d => {
      d % 2 match {
        // 并不是真的分流了，还是一个data stream，而是打了标签，然后可以用select选择
        case 0 => List("first")
        case 1 => List("second")
      }
    })
    // select 通过标签获取指定流
    splitStream.select("first").print.setParallelism(1)

    env.execute
  }

}
