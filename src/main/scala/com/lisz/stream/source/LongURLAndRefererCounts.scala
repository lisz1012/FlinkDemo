package com.lisz.stream.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer}
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringDeserializer

// 读取Kafka key和value
object LongURLAndRefererCounts {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties
    properties.setProperty("bootstrap.servers", "Kafka_1:9092,Kafka_2:9092,Kafka_3:9092")
    // 注意当初在讲 spark streaming + Kafka receiver模式，要去传zk 的url（元数据）
    properties.setProperty("group.id", "flink-group-001")
    properties.setProperty("key.deserializer", classOf[StringDeserializer].getName)
    properties.setProperty("value.deserializer", classOf[StringDeserializer].getName)

    val refererTag = new OutputTag[(String, Int)]("Referer")

    val stream = env.addSource(new FlinkKafkaConsumer[String]("long-url", new SimpleStringSchema(), properties)) //(new FlinkKafkaConsumer[(String, String)]())
    // processStream是主流, 关于long-url的统计
    val processStream = stream.process(new ProcessFunction[String, (String, Int)] {
      override def processElement(value: String, ctx: ProcessFunction[String, (String, Int)]#Context, out: Collector[(String, Int)]): Unit = {
        // http://www.google.com no_referer
        val splits = value.split("\\s+")
        out.collect(splits(0), 1)
        ctx.output(refererTag, (splits(1), 1))
      }
    })
    val refererStream = processStream.getSideOutput(refererTag)

    processStream.keyBy(0).sum(1).print
    refererStream.keyBy(0).sum(1).print

    env.execute
  }

}
