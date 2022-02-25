package com.lisz.stream.transformation2

import java.util.Properties

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTuple2TypeInformation, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer

object CarFlowAnalysis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val props = new Properties()
    props.setProperty("bootstrap.servers", "hadoop-04:9092,hadoop-02:9092,hadoop-03:9092")
    props.setProperty("key.deserializer", classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer", classOf[StringDeserializer].getName)
    props.setProperty("group.id", "flink-group-001")
    val schema = new KafkaDeserializationSchema[(String, String)] {
      override def isEndOfStream(nextElement: (String, String)): Boolean = {
        false
      }
      override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): (String, String) = {
        (new String(record.key()), new String(record.value()))
      }
      override def getProducedType: TypeInformation[(String, String)] = {
        createTuple2TypeInformation(createTypeInformation[String], createTypeInformation[String])
      }
    }
    val stream = env.addSource(new FlinkKafkaConsumer[(String, String)]("flink-kafka", schema, props))
    val valueStream = stream.map(_._2)
    // stream中元素类型：变成二元组类型kv stream   k: monitor_id, v: 1
    /*
      相同key的数据一定是由某一个sub-task来处理的，一个sub-task就是一个线程，
      一个subTask可以处理多个key所对应的数据
     */
    valueStream.map(data=>{
      val splits = data.split("\\s+")
      (splits(0), 1)
    }).keyBy(_._1).reduce(new ReduceFunction[(String, Int)] {
      // value1是截止到上一次的状态或结果，value2是本次要聚合的数据
      override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
        (value1._1, value1._2 + value2._2)
      }
    }).print
    env.execute
  }
}
