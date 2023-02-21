package com.lisz.stream.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

import scala.util.Random

// 自定义数据源. 自己发射自己接收
object CustomSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // [String] 代表发射的数据类型/
    // 如果是机遇SourceFunction接口实现自定义的数据源，则这个数据源只支持单线程
    val stream = env.addSource[String](new SourceFunction[String] {
      var flag = true

      // 发射数据. 读取任何地方（例如Redis）的数据，然后将其发射出去
      override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        val random = new Random()
        while (flag) {
          ctx.collect("Hello " + random.nextInt(100)) // 发射数据
          Thread.sleep(500)
        }
      }

      // 停止
      override def cancel(): Unit = flag = false
    })//.setParallelism(3)
    stream.print
    env.execute
  }

}
