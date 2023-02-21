package com.lisz.stream.source

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.flink.api.scala._

import scala.io.Source

object FlinkKafkaProducer {
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop-04:9092,hadoop-02:9092,hadoop-03:9092")
    properties.setProperty("key.serializer", classOf[StringSerializer].getName)
    properties.setProperty("value.serializer", classOf[StringSerializer].getName)

    val producer = new KafkaProducer[String, String](properties)
    // 不要轻易调用这个方法, 因为他会把文件中的所有数据都封装到一个集合里面去
    val iter = Source.fromFile("./data/data_carFlow_all_column_test.txt").getLines()
    for (i <- 1 to 100) {
      for (elem <- iter) {
        // 0235 只要卡口号、车牌号、时间戳、车速
        val splits = elem.split(",")
        val monitorId = splits(0).replace("'", "")
        val carId = splits(2).replace("'", "")
        val timestamp = splits(4).replace("'", "")
        val speed = splits(6)
        val builder = new StringBuilder
        builder.append(monitorId + "\t").append(carId + "\t").append(timestamp + "\t").append(speed)
        val info = builder.toString()
        // 往Kafka里面生产的数据就是一个kv对
        producer.send(new ProducerRecord[String, String]("flink-kafka", i + "", info))
        // println(info)
        Thread.sleep(500)
      }
    }

  }

}
