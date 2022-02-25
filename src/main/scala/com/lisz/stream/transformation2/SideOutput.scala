package com.lisz.stream.transformation2

import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector

object SideOutput {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("hadoop-01", 8888)
    val gtTag = new OutputTag[String]("gt") // 侧输出流还是对不同的数据打了个标签
    val ltTag = new OutputTag[String]("lt") // 侧输出流还是对不同的数据打了个标签
    // process可以理解成是一个flink的算子，process是一个比较底层的API，可以拿到很多map、flatMap拿不到的底层API
    val processStream = stream.process(new ProcessFunction[String, String] {
      override def processElement(value: String, ctx: ProcessFunction[String, String]#Context, out: Collector[String]): Unit = {
        val longVar = value.toLong
        try {
          if (longVar > 100) {
            out.collect(value)       // 主流
          } else if (longVar > 50){
            ctx.output(gtTag, value) // 副流1
          } else {
            ctx.output(ltTag, value) // 副流2
          }
        } catch {
          case e => e.getMessage
            ctx.output(gtTag, value)
        }
      }
    })
    processStream.getSideOutput(gtTag).map(x=>x + " gt").print  // 打印ctx里的gt，副流1
    processStream.getSideOutput(ltTag).map(x=>x + " lt").print   // 打印ctx里的lt，副流2
    processStream.print   // 默认只打印主流： out 的数据

    env.execute
  }

}
