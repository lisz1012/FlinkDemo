package com.lisz.stream.transformation2

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/**
 * 定时器场景：数据延迟，不要flink立马处理。银行里的对账系统
 * app
 * app成功  银行对账也要成功，两个流有时间差。
 * 5s的时候查看一下，如果都没有转账记录，就认为转账失败
 */
object MonitorSpeeding {
  case class CarInfo(carId: String, speed: Long)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("hadoop-01", 8888)
    stream.map(data=>{
      val splits = data.split("\\s+")
      val carId = splits(0)
      val speed = splits(1).toLong
      CarInfo(carId, speed)
    }).keyBy(_.carId).process(new KeyedProcessFunction[String, CarInfo, String] {
      override def processElement(value: CarInfo, ctx: KeyedProcessFunction[String, CarInfo, String]#Context, out: Collector[String]): Unit = {
        if (value.speed > 100) {
          val currentTime = ctx.timerService().currentProcessingTime
          ctx.timerService().registerProcessingTimeTimer(currentTime + 3000)
        }
      }
      override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, CarInfo, String]#OnTimerContext, out: Collector[String]): Unit = {
        out.collect(s"warn ... time: $timestamp carId: ${ctx.getCurrentKey}") // collect就是往下游发射数据
      }
    }).print
    env.execute
  }
}
