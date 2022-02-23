package com.lisz.stream.transformation2

import java.lang

import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable.ListBuffer

// split算子可以根据某些条件拆分数据流
object SplitOperator {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 偶数分到一个流(first)中，奇数（second）去另一个流
    val stream = env.generateSequence(1, 100)
    val splitString = stream.split(d => {
      d % 2 match {
        case 0 => List("first")
        case 1 => List("second")
      }
    })
    // select 算子可以通过标签获取指定的流
    val first = splitString.select("first")
    val second = splitString.select("second")
//    first.print.setParallelism(1)
//    println("===================")
    second.print.setParallelism(1)

    env.execute
  }
}
