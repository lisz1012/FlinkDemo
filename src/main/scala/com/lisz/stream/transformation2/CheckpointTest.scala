package com.lisz.stream.transformation2

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object CheckpointTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9))
    stream.keyBy(x => x % 2).reduce(_+_).print // 肯定只有俩分区.
    env.execute
  }
}
