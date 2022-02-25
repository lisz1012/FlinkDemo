package com.lisz.stream.transformation2

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.CoMapFunction

/**
 * connect算子也是将这两个数据流合并，优点：被合并的两个数据流中的数据类型可以不一致；union元素类型必须一致。connect合并之后的流还不能直接打印
 */
object ConnectOperator {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.socketTextStream("hadoop-01", 8888)
    val ds2 = env.socketTextStream("hadoop-01", 9999)
    val wcStream1 = ds1.flatMap(_.split("\\s+")).map((_, 1)).keyBy(0).sum(1)
    val wcStream2 = ds2.flatMap(_.split("\\s+")).map((_, 1)).keyBy(0).sum(1)
    val connected = wcStream2.connect(wcStream1)

    connected.map(new CoMapFunction[(String, Int), (String, Int), (String, Int)] {
      // 只处理wcStream2中的元素
      override def map1(value: (String, Int)): (String, Int) = {
        println("wcStream2: " + value)
        value
      }
      // 只处理wcStream1中的元素
      override def map2(value: (String, Int)): (String, Int) = {
        println("wcStream1: " + value)
        value
      }
    })

    env.execute
  }
}
