package com.lisz.stream.transformation2

import org.apache.flink.api.common.functions.{RichAggregateFunction, RichMapFunction, RichReduceFunction}
import org.apache.flink.api.common.state.{AggregatingState, AggregatingStateDescriptor, ReducingState, ReducingStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

/**
 * 统计每辆车的速度总和
 */
object AggregatingStateTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[String] = env.socketTextStream("localhost", 8888)
    stream.map(data => {
      val arr = data.split(" ")
      (arr(0), arr(1).toLong)
    }).keyBy(_._1)
      .map(new RichMapFunction[(String, Long), (String, Long)] {
        private var speedCount: AggregatingState[Long, Long] = _
        override def open(parameters: Configuration): Unit = {
          val desc = new AggregatingStateDescriptor[Long, Long, Long]("agg", new RichAggregateFunction[Long, Long, Long] {
            // 初始化一个累加器 spark combineByKy的第一个函数
            override def createAccumulator(): Long = 0
            // 每来一条数据会调用一次  spark combineByKy第二个函数
            override def add(value: Long, accumulator: Long): Long = accumulator + value

            override def getResult(accumulator: Long): Long = accumulator
            // spark combineByKy第三个函数
            override def merge(acc1: Long, acc2: Long): Long = acc1 + acc2  // a和b是两个不同的累加器
          }, createTypeInformation[Long])
          speedCount = getRuntimeContext.getAggregatingState(desc)
        }

        override def map(value: (String, Long)): (String, Long) = {
          speedCount.add(value._2)
          (value._1, speedCount.get())
        }
      })
      .print
    env.execute
  }
}
