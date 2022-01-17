package com.lisz.stream.transformation

import org.apache.flink.api.common.functions.{RichFilterFunction, RichMapFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import redis.clients.jedis.Jedis

/**
 * 将WoudCount的结果写入Redis中
 */
object WC2Redis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("hadoop-01", 8888)
    val wcStream = stream.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1)
    wcStream.map(new RichMapFunction[(String, Int), String] {
      private var jedis:Jedis = _
      // SubTask启动的时候，首先调用的方法
      override def open(parameters: Configuration): Unit = {
        // 获取Task的名字
        val name = getRuntimeContext.getTaskName
        // 获取子任务的名字
        val subtaskName = getRuntimeContext.getTaskNameWithSubtasks
        println(s"TaskName: $name Sub-Task Name: $subtaskName")
        jedis = new Jedis("redis", 6379)
        jedis.select(3)
      }

      // 执行完毕前，调用的方法。生命周期方法
      override def close(): Unit = {
        println("closing")
        jedis.close()
      }

      // 处理每一个元素的
      override def map(value: (String, Int)): String = {
        jedis.set(value._1, value._2.toString)
        value._1
      }
    })

//    stream.filter(new RichFilterFunction[] {
//
//    })

    env.execute
  }

}
