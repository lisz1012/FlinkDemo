package com.lisz.stream.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

import scala.collection.mutable.ListBuffer

/**
 * 要启动com.lisz.stream.source.FlinkKafkaProducer和Kafka消息队列.
 * WordCount 结果写入MySQL。MySQL原生不支持幂等操作
 * 代码实现幂等操作
 * flink 1
 * flink 2 -> flink 3 (先尝试update，更新不成功或者更新0条数据，才insert)。
 * flink 3
 *
 * MySQL不是Flink内嵌支持的，所以需要自定义Sink、导入MySQL驱动包
 */
object MySQLSinkTest {
 def main(args: Array[String]): Unit = {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val stream = env.socketTextStream("hadoop-01", 8888)
  stream.flatMap(line => {
    val rest = new ListBuffer[(String, Int)]
    line.split("\\s+").foreach(word => rest += ((word, 1)))
    rest
  }).keyBy(0)
    .reduce((v1: (String, Int), v2: (String, Int)) => {
     (v1._1, v1._2 + v2._2)
    })
    .addSink(new RichSinkFunction[(String, Int)] {
      var conn:Connection = _
      var updatestmt: PreparedStatement = _
      var insertstmt: PreparedStatement = _
      // 每过来一个元素会被调用一次
      override def invoke(value: (String, Int), context: SinkFunction.Context[_]): Unit = {
        updatestmt = conn.prepareStatement("update wc set count = ? where word = ?")
        updatestmt.setString(2, value._1)
        updatestmt.setInt(1, value._2)
        if (updatestmt.executeUpdate == 0) {
          insertstmt = conn.prepareStatement("insert into wc values (?, ?)")
          insertstmt.setString(1, value._1)
          insertstmt.setInt(2, value._2)
          insertstmt.execute
        }
      }

      // 开始执行业务逻辑之前会调用一次。
      override def open(parameters: Configuration): Unit = {
        Class.forName("com.mysql.cj.jdbc.Driver")
        conn = DriverManager.getConnection("jdbc:mysql://mysql-1:3306/test", "root", "P@ssw0rd")
      }

      // 正常业务执行完毕会被调用一次
      override def close(): Unit = {
        updatestmt.close
        insertstmt.close
        conn.close
      }
    })
   env.execute
 }
}
