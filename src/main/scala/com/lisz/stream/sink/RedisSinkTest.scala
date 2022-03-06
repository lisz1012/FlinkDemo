package com.lisz.stream.sink

import java.net.InetSocketAddress

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisClusterConfig, FlinkJedisPoolConfig}
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/*
  将WordCount结果写入Redis中. ⚠️ Flink是一个流式计算框架，往Redis里面存数据的操作必须是幂等操作
 */
object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("hadoop-01", 8888)

    val builder = new FlinkJedisPoolConfig.Builder
    val config = builder.setHost("redis-1").setPort(6379).build()

    stream.flatMap(line => {
      val rest = new ListBuffer[(String, Int)]
      line.split("\\s+").foreach(word => rest += ((word,1)))
      rest
    }).keyBy(0)
      .reduce((v1: (String, Int), v2: (String, Int)) => {
        (v1._1, v1._2 + v2._2)
      })
      .addSink(new RedisSink[(String, Int)](config, new RedisMapper[(String, Int)] {
        override def getCommandDescription: RedisCommandDescription = {
          new RedisCommandDescription(RedisCommand.HSET, "wc")
        }

        override def getKeyFromData(data: (String, Int)): String = {
          data._1
        }

        override def getValueFromData(data: (String, Int)): String = {
          data._2.toString
        }
      }))
    env.execute
  }
}
