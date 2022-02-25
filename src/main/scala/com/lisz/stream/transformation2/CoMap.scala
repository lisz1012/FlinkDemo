package com.lisz.stream.transformation2

import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.CoMapFunction

import scala.collection.mutable
import org.apache.flink.api.scala._

object CoMap {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) // 必须设置为1，否则下面map可能在map2里明明都加载了，但是在map1里面还是显示map为空
    val filePath = "data/cardId2Name"
    val card2NameStream = env.readFile(new TextInputFormat(new Path(filePath)), filePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 1000)
    val dataStream = env.socketTextStream("hadoop-01", 8888)
    dataStream.connect(card2NameStream).map(new CoMapFunction[String, String, String] { // 第一个、第二个流的类型和返回类型
      // 每一个Thread都要保存一个HashMap， 不建议用这种写法
      private val map = new mutable.HashMap[String, String]()
      override def map1(value: String): String = { // 处理 . 前面的那个流：卡口数据流
        map.getOrElse(value, "name not found")
      }
      override def map2(value: String): String = { // 处理 . 后面的那个流：配置文件流，每次文件变更都会推送整个文件
        val splits = value.split(" ")
        map.put(splits(0), splits(1))
        value + " 加载完毕 ..."
      }
    }).print
    env.execute
  }

}
