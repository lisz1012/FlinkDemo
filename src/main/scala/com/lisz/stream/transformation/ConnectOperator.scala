package com.lisz.stream.transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.{CoFlatMapFunction, CoMapFunction}
import org.apache.flink.util.Collector

/**
 * connect 算子也是将两个数据流进行合并
 * 优点：被合并的这两个数据流中的元素类型可以不一样 union的元素类型必须一致
 * 合并后的流类型不能被print直接打印
 */
object ConnectOperator {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.socketTextStream("hadoop-01", 8888)
    val ds2 = env.socketTextStream("hadoop-01", 9999)
    val wcStream1 = ds1.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1)
    val wcStream2 = ds2.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1)
    val connectedStream = wcStream2.connect(wcStream1)
    connectedStream.map(new CoMapFunction[(String, Int), (String, Int), (String, Int)] {
      // 处理wcStream2（前面的）中的元素
      override def map1(value: (String, Int)): (String, Int) = {
        println("wcStream2: " + value)
        value
      }

      // 处理wcStream1（后面的）中的元素
      override def map2(value: (String, Int)): (String, Int) = {
        println("wcStream1: " + value)
        value
      }
    })

//    connectedStream.flatMap(new CoFlatMapFunction[String, String, (String, Int)] {
//      override def flatMap1(value: String, out: Collector[(String, Int)]): Unit = ???
//
//      override def flatMap2(value: String, out: Collector[(String, Int)]): Unit = ???
//    }, new CoFlatMapFunction[String, String, (String, Int)] {
//      override def flatMap1(value: String, out: Collector[(String, Int)]): Unit = ???
//
//      override def flatMap2(value: String, out: Collector[(String, Int)]): Unit = ???
//    })

    env.execute
  }
}
