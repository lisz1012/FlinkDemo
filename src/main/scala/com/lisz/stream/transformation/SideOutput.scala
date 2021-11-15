package com.lisz.stream.transformation

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

object SideOutput {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("hadoop-01", 8888)
    val excellentTag = new OutputTag[String]("Excellent")
    val failedTag = new OutputTag[String]("Failed")
    // process可以理解成Flink的一个算子，process属于底层的API，可以拿到很多map、flatMap所拿不到的信息
    val processStream = stream.process(new ProcessFunction[String, String] {
      // 处理每一个元素：value，Collector负责往下游发射
      override def processElement(value: String, ctx: ProcessFunction[String, String]#Context, out: Collector[String]): Unit = {
        val score = value.toInt
        if (score < 60) {
          ctx.output(failedTag, value)
        } else if (score > 89) {
          ctx.output(excellentTag, value)
        } else {
          out.collect(value) // 往主流发射
        }
      }
    })
    // 获取测流数据
    val excellentStream = processStream.getSideOutput(excellentTag)
    val failedStream = processStream.getSideOutput(failedTag)
    // 默认值打印除了测流之外的数据（主流）
    processStream.print("Main")
    // 非主流。打印哪个流就个给他一个前缀
    excellentStream.print("Excellent")
    failedStream.print("Failed")
    env.execute
  }
}
