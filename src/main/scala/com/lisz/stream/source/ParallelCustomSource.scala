package com.lisz.stream.source

import java.util.Random

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

// 自定义一个多并行度的数据源
object ParallelCustomSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.addSource[String](new ParallelSourceFunction[String] { //或者 RichParallelSourceFunction
      var flag = true
      override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        val random = new Random
        while (flag) {
          ctx.collect("hello, " + random.nextInt(100))
          Thread.sleep(500)
        }

      }

      override def cancel(): Unit = flag = false
    }).setParallelism(2)
    stream.print.setParallelism(2) // 都设置成2,才能只是用两个线程来打印, 或者env.setParallelism(2)
    env.execute
  }

}
