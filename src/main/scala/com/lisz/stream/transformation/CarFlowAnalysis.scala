package com.lisz.stream.transformation

import java.util.Properties

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.flink.api.scala._

/**
 * 1。从Kafka中消费数据，统计各个卡口的流量
 * 数据如车流，一条条地来，每来一条立马处理。如果有n个线程处理reduce算子，每个线程里会保存一个key上一次聚合的结果，
 * 每个线程并没有维护太多的数据，很多都立刻就聚合简并了，这不是个批计算，而是流计算。key的分配跟Kafka或者Spark窄依赖
 * 一样：一个key只能由某一个线程（sub task）处理；一个线程可以处理多个key, 但是一个key不能被多个线程所处理
 *
 * 2。从Kafka中消费数据，统计每一分钟每一卡口的流量：把时间应设成分钟字段，应设成word count
 *
 */
object CarFlowAnalysis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties
    properties.setProperty("bootstrap.servers", "hadoop-02:9092,hadoop-03:9092,hadoop-04:9092")
    properties.setProperty("key.deserializer", classOf[StringDeserializer].getName)
    properties.setProperty("value.deserializer", classOf[StringDeserializer].getName)
    properties.setProperty("group.id", "flink-group-001")

    val stream = env.addSource(new FlinkKafkaConsumer[String]("flink-kafka", new SimpleStringSchema(), properties))
    stream.map(x => {
      println(x)
      val splits = x.split("\t")
      val timeMinute = splits(2)
      println(timeMinute)
      (timeMinute.substring(0, timeMinute.length - 3), 1) // 把时间映射成分钟字段，做wc

      // val splits = x.split("\t")
      // (splits(0), 1)
    }).keyBy(_._1).reduce(new ReduceFunction[(String, Int)] {
      override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
        (value1._1, value1._2 + value2._2)
      }
    }).print // keyBy(0).sim(1)
    env.execute
  }

}
