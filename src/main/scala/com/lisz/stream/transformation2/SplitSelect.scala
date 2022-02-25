package com.lisz.stream.transformation2

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object SplitSelect {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.generateSequence(1, 100)
    stream.split(d=>{
      d % 2 match {
        case 0 => List("even") // 对不同的数据打标签，然后对他用select选择
        case 1 => List("odd")
      }
    }).select("even").print
    env.execute
  }
}
