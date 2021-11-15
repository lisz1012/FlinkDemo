package com.lisz.stream.transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

/**
 * 应用场景：数字计算出Fibo数列，或者机器学习训练，因为要不断迭代
 */
object Iterate {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val initStream = env.socketTextStream("hadoop-01", 8888)
    val stream = initStream.map(_.toInt)
    /**
     * iterate 算子提供了对数据流迭代的支持。迭代由两部分组成：迭代题和终止迭代条件
     * 不满足终止迭代条件的数据流会返回到stream流中，进行下一次迭代
     * 满足终止条件的数据流继续往下游发送
     */
    stream.iterate(
      iteration => { // iteration 参数代表一个DataStream
        val iterationBody = iteration.map(x => { // 迭代体
          println("--- " + x)
          if (x > 0) {
            x - 1
          } else {
            x
          }
        })
        (iterationBody.filter(_ > 0), iterationBody.filter(_ <= 0)) // 第一个流会再次被传回到迭代体，第二个则会发射到下游去
      }).print()
    env.execute()
  }
}
