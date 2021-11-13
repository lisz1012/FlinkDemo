package com.lisz.stream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object ReadCollections {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 一般不用
    var stream:DataStream[Int] = env.fromCollection(List(1, 2, 3, 4, 5))
    stream.print
    stream = env.fromElements(1, 2, 3, 4, 5)
    stream.print
    env.execute
    // 默认会用满机器上所有的线程
  }

}
