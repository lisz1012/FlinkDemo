package com.lisz.stream.transformation2

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object KeyByOperator {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("hadoop-01", 8888)
    stream.flatMap(_.split(" ")).map((_,1))

//      .keyBy(new KeySelector[(String, Int), String] {
//        override def getKey(value: (String, Int)): String = value._1
//      })

      .keyBy(x=>x._1)

//      .sum(1)
      .reduce((v1, v2) => { // flink去重怎么做？v1是上一次聚合后的结果，v2是本次的传入数据，都是key(_.1)相同的二元组。
        (v1._1, v1._2 + v2._2)
      })
      .print


    env.execute
  }
}
