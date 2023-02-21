package com.lisz.stream.transformation

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer

object MapOperator {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val stream= env.generateSequence(1, 100) //测试用，产生一个1到100的数据流
    val stream = env.socketTextStream("hadoop-01", 8888)
    stream.map(_+"---------")
    // 如何使用flatMap来代替filter
    // 数据中包含了abc，那么就把它过滤掉.flatMap扁平化：map + flat 并返回集合
//    stream.flatMap(x => {
//      val list = new ListBuffer[String]
//      if (!x.contains("abc")) {
//        list += x
//      }
//      list.iterator
//    }).print

    // keyBy分流算子，根据用户指定的字段来分组
    stream.flatMap(_.split(" ")).map((_, 1))
//      .keyBy(new KeySelector[(String, Int), String] { // map出来的是个二元组，根据单词来分组，所以后面一个是String
//      override def getKey(value: (String, Int)): String = {
//        value._1
//      }
      .keyBy(_._1) // 或者 keyBy(0) 这一步和下一步相当于spark中的reduceByKey
      .reduce((v1, v2) => { //.sum(0)
        (v1._1, v1._2 + v2._2) // Flink的key有冗余，这是因为它是一个流计算，拿不到一个key里对应的一大堆的value数据
      }).print

    env.execute
  }

}
