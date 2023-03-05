package com.lisz.stream.transformation2

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object FileSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.socketTextStream("hadoop-01", 8888)

    val policy: DefaultRollingPolicy[String, String] = DefaultRollingPolicy.create()  // 两个泛型，第一个是数据类型，第二个是痛的ID的类型
      .withInactivityInterval(5000) //5秒没有写入新数据就产生一个新文件
      .withMaxPartSize(256 * 1024 * 1024) // 大小超过256MB也会产生一个小文件
      .withRolloverInterval(10000) // 文件的打开时间超过10s，也会滚动产生一个小文件
      .build()

    val builder = StreamingFileSink.forRowFormat(new Path("/Users/shuzheng/IdeaProjects/FlinkDemo/data"),
      new SimpleStringEncoder[String]("UTF-8"))

    val sink = builder
      .withBucketCheckInterval(100) // 每隔多长时间要去检测一次桶中的数据是否需要滚动
      .withRollingPolicy(policy)
      .build() // 文件的大小、打开时间、不活跃的时间

    stream.addSink(sink)

    env.execute()
  }
}
