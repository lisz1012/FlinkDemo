package com.lisz.stream.sink

import java.util.{Date, Properties}

import com.lisz.stream.util.DateUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.flink.api.scala._

/**
 * 统计各个卡口流量，将结果写入到HBase中
 */
object HBaseSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val props = new Properties
    props.setProperty("bootstrap.servers", "hadoop-02:9092")
    props.setProperty("key.deserializer", classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer", classOf[StringDeserializer].getName)
    props.setProperty("group.id", "flink-kafka-001")

    val stream = env.addSource(new FlinkKafkaConsumer[String]("flink-kafka", new SimpleStringSchema(), props))
    stream.map(line => {
      val splits = line.split("\\s+")
      (splits(0), 1)
    }).keyBy(0)
      .reduce((v1: (String, Int), v2: (String, Int)) => {
        (v1._1, v1._2 + v2._2)
      })
      .process(new ProcessFunction[(String, Int), (String, Int)] { // 数据处理过程中的算子，sink是最终发射的算子，数据处理过程中也可以写入数据库
        // rowKey: monitorId 列名：分钟 value： 流量
        var table: Table = _
        override def open(parameters: Configuration): Unit = {
          val conf = HBaseConfiguration.create()
          conf.set("hbase.zookeeper.quorum", "hadoop-02,hadoop-03,hadoop-04")
          conf.set("hbase.zookeeper.property.clientPort", "2181")
          conf.set("zookeeper.znode.parent", "/hbase")
          val tableName = "car_flow"
          val connection = ConnectionFactory.createConnection(conf)
          table = connection.getTable(TableName.valueOf(tableName))
        }

        override def close(): Unit = {
          table.close
        }

        override def processElement(value: (String, Int), ctx: ProcessFunction[(String, Int), (String, Int)]#Context, out: Collector[(String, Int)]): Unit = {
          val min = DateUtils.getMin(new Date())
          val monitorId = value._1
          val put = new Put(Bytes.toBytes(monitorId))
          put.addColumn("count".getBytes, Bytes.toBytes(min), Bytes.toBytes(value._2))
          table.put(put)
        }
      })
    env.execute
  }
}
