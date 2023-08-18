package com.lisz.stream.transformation2

import org.apache.flink.api.common.functions.{RichMapFunction, RichReduceFunction}
import org.apache.flink.api.common.state.{ReducingState, ReducingStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

/**
 * 统计每辆车的速度总和
 */
object ReducingStateTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[String] = env.socketTextStream("localhost", 8888)
    stream.map(data => {
      val arr = data.split(" ")
      (arr(0), arr(1).toLong)
    }).keyBy(_._1)
      .map(new RichMapFunction[(String, Long), (String, Long)] {
        private var speedCount: ReducingState[Long] = _
        override def open(parameters: Configuration): Unit = {
          val desc = new ReducingStateDescriptor[Long]("xxx", new RichReduceFunction[Long] {
            override def reduce(value1: Long, value2: Long): Long = { // value1 是上次聚合的状态, value2 是当前加入的元素
              value1 + value2
            }
          }, createTypeInformation[Long])
          speedCount = getRuntimeContext.getReducingState(desc)
        }

        override def map(value: (String, Long)): (String, Long) = {
          speedCount.add(value._2) // 这里每添加一个元素,就会调用一次上面的 reduce 方法, 往下追代码, 会看到ReduceFunction.reduce()
          (value._1, speedCount.get())
        }
      })
      .print
    env.execute
  }
}
