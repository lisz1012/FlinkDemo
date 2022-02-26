package com.lisz.stream.transformation2

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import redis.clients.jedis.{HostAndPort, Jedis}

object WC2Redis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("hadoop-01", 8888)
    val restStream = stream.flatMap(_.split("\\s+")).map((_, 1)).keyBy(0).sum(1)
    restStream.map(new RichMapFunction[(String, Int), String] {
      var jedis: Jedis = _
      // open这个方法在线程或者sub-task被启动的时候首先被调用
      override def open(parameters: Configuration): Unit = {
        val context = getRuntimeContext
        val name = context.getTaskName
        val subtaskName = context.getTaskNameWithSubtasks
        println(s"$name -- $subtaskName")
        jedis = new Jedis(new HostAndPort("redis-1", 6379))
        jedis.select(3)
      }
      override def close(): Unit = {
        jedis.close()
      }
      override def map(value: (String, Int)): String = {
        jedis.set(value._1, value._2+"") // 变成字符串
        value._1
      }
    })
    env.execute
  }
}
