package com.lisz.stream.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTuple2TypeInformation, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer

// 读取Kafka key和value
object ReadKafka2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties
    properties.setProperty("bootstrap.servers", "Kafka_1:9092,Kafka_2:9092,Kafka_3:9092")
    // 注意当初在讲 spark streaming + Kafka receiver模式，要去传zk 的url（元数据）
    properties.setProperty("group.id", "flink-group-001")
    properties.setProperty("key.deserializer", classOf[StringDeserializer].getName)
    properties.setProperty("value.deserializer", classOf[StringDeserializer].getName)

    val stream = env.addSource(new FlinkKafkaConsumer[String]("long-url", new SimpleStringSchema(), properties)) //(new FlinkKafkaConsumer[(String, String)]())
    stream.print
    env.execute
  }

}
