package com.lisz.stream.source

import java.util.Properties
import java.util.concurrent.DelayQueue

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.flink.api.scala._

// Flink不消费Kafka key
object ReadKafkaNoKey {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties
    properties.setProperty("bootstrap.servers", "hadoop-02:9092,hadoop-02:9092,hadoop-03:9092")
    properties.setProperty("key.deserializer", classOf[StringDeserializer].getName)
    properties.setProperty("value.deserializer", classOf[StringDeserializer].getName)
    properties.setProperty("group.id", "flink-kafka-001")

    val stream = env.addSource(new FlinkKafkaConsumer[String]("flink-kafka", new SimpleStringSchema(), properties))
    stream.print
    DelayQueue
    env.execute
  }
}
