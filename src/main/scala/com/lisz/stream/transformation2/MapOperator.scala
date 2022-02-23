package com.lisz.stream.transformation2

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable.ListBuffer

object MapOperator {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("hadoop-01", 8888)
//    val stream = env.generateSequence((1, 100)) // 产生1 - 100的数据集合
    stream.map(x=>x+"-----")
    // 水涌flatMap来代替filter，数据中包含了"abc"就过滤掉，flatMap算子：先map再lfat扁平化。flatMap返回一个集合，flatMap代替filter的时候，集合为空
    stream.flatMap(x=>{
      val rest = new ListBuffer[String]
      if (!x.contains("abc")) rest += x
      rest // rest.iterator 也行
    }).print
    env.execute
  }

}
