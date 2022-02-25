package com.lisz.stream.transformation2

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

object Iterate {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val initStream = env.socketTextStream("hadoop-01", 8888)
    val stream = initStream.map(_.toLong)
    stream.iterate(
      iteration => { // iteration 参数代表的是一个data stream -- （循环♻️）迭代流
        val iterationBody = iteration.map(x => {
          println(x)
          if (x > 0) x - 1
          else x
        })
        (iterationBody.filter(_ > 0), iterationBody.filter(_ <= 0)) // 符合第一个条件的元素会再次被传回到迭代体，符合第二个的则会发射到下游去。可以用来做ML训练
      }
    ).print
    env.execute
  }

}
