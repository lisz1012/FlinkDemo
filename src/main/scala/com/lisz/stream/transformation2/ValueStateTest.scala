package com.lisz.stream.transformation2

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object ValueStateTest {
  case class CarInfo(carId: String, speed: Long)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("localhost", 8888)
    stream.map(data=>{
      val strs = data.split(" ")
      CarInfo(strs(0), strs(1).toLong)
    }).keyBy(_.carId)
      .map(new RichMapFunction[CarInfo, String] { // 泛型是输入（上面keyBy所操作的_的类型）和输出类型
      // 下面如果定义普通的变量，则并不会持久化到外部存储，下次重启程序的时候则会从0开始
      private var lastTempState: ValueState[Long] = _
      override def open(parameters: Configuration): Unit = {
        val desc = new ValueStateDescriptor[Long]("lastSpeed", createTypeInformation[Long])
        lastTempState = getRuntimeContext.getState(desc)
      }
      override def map(value: CarInfo): String = {
        val lastSpeed = lastTempState.value()
        lastTempState.update(value.speed)
        if (lastSpeed != 0 && value.speed - lastSpeed > 30) {
          "Over speed " + value.toString
        } else
            value.carId
      }
    }).print
    env.execute
  }
}
