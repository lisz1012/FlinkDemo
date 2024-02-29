package com.lisz.stream.source

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

import scala.io.Source

object FlinkKafkaProducer2 {
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.1.11:9092,192.168.1.12:9092,192.168.1.13:9092")
    properties.setProperty("key.serializer", classOf[StringSerializer].getName)
    properties.setProperty("value.serializer", classOf[StringSerializer].getName)

    val producer = new KafkaProducer[String, String](properties)
    // send是个异步，所以要么睡一会儿，要么Future.get一下，以防止还没send出去main就结束了，结果就跟没运行一样.
    val future = producer.send(new ProducerRecord[String, String]("long-url", "aaabbb localhost"))
    val recordMetadata = future.get

    println(recordMetadata.partition() + " - " + recordMetadata.offset() + " --- " + recordMetadata.topic())
  }

}
