package com.lisz.stream.sink

import java.lang
import java.util.Properties

import akka.remote.serialization.StringSerializer
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.clients.producer.ProducerRecord

import scala.collection.mutable.ListBuffer

object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("hadoop-01", 8888)
    val props = new Properties()
    props.setProperty("bootstrap.servers", "hadoop-02:9092")
//    props.setProperty("key.serializer", classOf[StringSerializer].getName)
//    props.setProperty("value.serializer", classOf[StringSerializer].getName)

    stream.flatMap(line => {
      val rest = new ListBuffer[(String, Int)]
      line.split("\\s+").foreach(word => {
        rest += ((word, 1))
      })
      rest
    }).keyBy(0)
      .reduce((v1: (String, Int), v2: (String, Int)) => {
        (v1._1, v1._2 + v2._2)
      })
      .addSink(new FlinkKafkaProducer[(String, Int)]("flink-kafka", new KafkaSerializationSchema[(String, Int)] {
        override def serialize(element: (String, Int), timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
          new ProducerRecord[Array[Byte], Array[Byte]]("flink-kafka", element._1.getBytes, element._2.toString.getBytes)
        }
      }, props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)) // 保证数据"恰好一次"发射到Kafka里面

    env.execute
  }

}
