package com.lisz.stream.transformation2

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.flink.api.scala._

object CarFlowPerMin {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val props = new Properties()
    props.setProperty("bootstrap.servers", "hadoop-02:9092")
    props.setProperty("key.deserializer", classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer", classOf[StringDeserializer].getName)
    props.setProperty("group.id", "flink-group-001")
    val stream = env.addSource(new FlinkKafkaConsumer[String]("flink-kafka", new SimpleStringSchema(), props))
    stream.map(data=>{
      val splits = data.split("\\s+")
      val monitorId = splits(0)
      monitorId + "_" + ""
    })
    env.execute
  }
}
