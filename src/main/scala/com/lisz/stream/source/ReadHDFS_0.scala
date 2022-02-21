package com.lisz.stream.source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * Flume -> HDFS -> Flink实时ETL，要给予readFile实时处理然后入数仓
 */
object ReadHDFS_0 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.readTextFile("hdfs://mycluster/flink/data/")
    ds.print
    env.execute
  }
}
