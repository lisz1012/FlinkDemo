package com.lisz.stream.transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/**
 * 定时器 应用场景：数据延迟。银行对账系统：真正成功：app修改成功了且银行数据也修改成功，两条流有数据延迟，5s的时候必须两条流里面都有
 * 这一条转账记录，否则认为转账失败
 */
object MonitorSpeeding {
  case class CarInfo(carId:String, speed:Int)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("hadoop-01", 8888)
    stream.map(x => {
      val splits = x.split(" ")
      CarInfo(splits(0), splits(1).toInt)
    }).keyBy(_.carId).process(new KeyedProcessFunction[String, CarInfo, String] { // 前面有keyBy，这里就得有Keyed
      // 只是注册一个一段时间之后的事件，具体到时候什么行为，则要看onTimer方法里的逻辑
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
