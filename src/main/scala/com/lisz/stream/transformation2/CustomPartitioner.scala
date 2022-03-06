package com.lisz.stream.transformation2

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

/*
  根据用户指定的字段来分发：根据指定字段的Hash来分发、自定义Partitioner
 */
object CustomPartitioner {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    val stream = env.generateSequence(1, 10).map((_, 1))
    stream.writeAsText("./data/stream8")
    stream.partitionCustom(new CustomPartitioner(), 0) // 以tuple的第1个参数为key
      .writeAsText("./data/stream9").setParallelism(4)
    env.execute
  }
}

class CustomPartitioner extends Partitioner[Long]{
  override def partition(key: Long, numPartitions: Int): Int = { // numPartitions是下游的分区数, setParallelism来确定的分区数（并行度，thread、subTask，一个task有多个subtasks）
    (key % numPartitions).toInt
  }
}