package com.lisz.stream.transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

object MonitorSpeeding {
  case class CarInfo(carId:String, speed:Int)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("hadoop-01", 8888)
    stream.map(x => {
      val splits = x.split(" ")
      CarInfo(splits(0), splits(1).toInt)
    }).keyBy(_.carId).process(new KeyedProcessFunction[String, CarInfo, String] {
      override def processElement(value: CarInfo, ctx: KeyedProcessFunction[String, CarInfo, String]#Context, out: Collector[String]): Unit = {
        val currentTime = ctx.timerService().currentProcessingTime
        if (value.speed > 100) {
          val timerTime = currentTime + 5 * 1000
          ctx.timerService().registerProcessingTimeTimer(timerTime)
        }
      }

      override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, CarInfo, String]#OnTimerContext, out : Collector[String]): Unit= {
        var warning = s"车牌号是${ctx.getCurrentKey}的车主您好, 您超速了"
        out.collect(warning)
      }
    }).print

    env.execute
  }

}
