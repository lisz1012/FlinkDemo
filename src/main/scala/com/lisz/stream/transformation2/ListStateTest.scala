package com.lisz.stream.transformation2

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.flink.streaming.api.scala._

import java.text.SimpleDateFormat
import java.util.Properties
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Properties

/**
 * 统计每一辆车的运行轨迹
 * 1。 拿到每一辆车的所有信息：车牌号、卡口号、eventTime、speed
 * 2。 根据每一辆车进新分组
 * 3。 对魅族数据中的信息按照eventtime排序，卡口连接起来
 * 这里是计算了整个历史的轨迹，一般不用，一般用window，只计算一段时间的
 */
object ListStateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val props = new Properties()
    props.setProperty("bootstrap.servers", "hadoop-02:9092")
    props.setProperty("group.id", "flink-kafka-001")
    props.setProperty("key.deserializer", classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer", classOf[StringDeserializer].getName)

    // 卡口号、车牌号、eventTime、车速
    val stream = env.addSource(new FlinkKafkaConsumer[String]("flink-kafka", new SimpleStringSchema(), props))
    val format = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss")
    stream.map(data=>{
      val arr = data.split("\t")
      val time = format.parse(arr(2)).getTime
      (arr(0), arr(1), time, arr(3).toLong)
    }).keyBy(_._2)
      .map(new RichMapFunction[(String, String, Long, Long), (String, String)] {
        // 有点像时序数据
        private var carInfos: ListState[(String, Long)] = _
        override def open(parameters: Configuration): Unit = {
          val desc = new ListStateDescriptor[(String, Long)]("list", createTypeInformation[(String, Long)])
          carInfos = getRuntimeContext.getListState(desc)
        }
        override def map(value: (String, String, Long, Long)): (String, String) = {
          carInfos.add((value._1, value._3))
          val seq = carInfos.get().asScala.seq
          val sortedList = seq.toList.sortBy(_._2)
          val builder = new StringBuilder
          for (elem <- sortedList) {
            builder.append(elem._1 + "\t") // elem._1是卡口号
          }
          (value._2, builder.toString())
        }
      }).print
    env.execute
  }
}
