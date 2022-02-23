package com.lisz.stream.transformation2

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

// Union可以将两个数据流合并，条件是：各个数据流中的数据类型必须一致
object UnionOperator {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream1 = env.fromCollection(List(1, 3, 5, 7, 9))
    val stream2 = env.fromCollection(List(2, 4, 6, 8, 10))
    val unioned = stream1.union(stream2)
    unioned.print

    env.execute
  }
}
