package com.lisz.stream

import java.util.Properties

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer

// 读取Kafka key和value
object ReadKafka {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties
    properties.setProperty("bootstrap.servers", "hadoop-04:9092,hadoop-02:9092,hadoop-03:9092")
    // 注意当初在讲 spark streaming + Kafka receiver模式，要去传zk 的url（元数据）
    properties.setProperty("group.id", "flink-group-001")
    properties.setProperty("key.deserializer", classOf[StringDeserializer].getName)
    properties.setProperty("value.deserializer", classOf[StringDeserializer].getName)
    val schema = new KafkaDeserializationSchema[(String, String)] {
      // 停止的条件是什么？
      override def isEndOfStream(nextElement: (String, String)): Boolean = false

      // 进行反序列化的字节流
      override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): (String, String) =
        (new String(record.key()), new String (record.value()))

      // 指定一下返回的数据类型，必须是Flink给我们提供的类型
      override def getProducedType: TypeInformation[(String, String)] = {
        createTuple2TypeInformation(createTypeInformation[String], createTypeInformation[String])
      }
    }
    val stream = env.addSource(new FlinkKafkaConsumer[(String, String)]("flink-kafka", schema, properties)) //(new FlinkKafkaConsumer[(String, String)]())
    stream.print
    env.execute
  }

}
