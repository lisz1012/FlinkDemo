package com.lisz.stream.transformation

import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.CoMapFunction

import scala.collection.mutable

/**
 * 实时监控配置文件中车牌号与车主的关系，所以要用readFile + FileProcessingMode.PROCESS_CONTINUOUSLY
 * 不可以用readTextFile
 */
object CoMap {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val filePath = "data/cardId2Name"
    val carId2NameStream = env.readFile(new TextInputFormat(new Path(filePath)), filePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 100)
    val dataStream = env.socketTextStream("hadoop-01", 8888)
    dataStream.connect(carId2NameStream).map(new CoMapFunction[String, String, String] { // 第一个输入类型、第二个输入类型，输出的类型
      // ‼️下面的写法每一个Thread都要保存一个HashMap， 不建议用这种写法
      private val map = new mutable.HashMap[String, String]
      override def map1(value: String): String = {
        map.getOrElse(value, "Name not found")  // 车和车主的关系配置数据流还没有处理完毕，卡口数据流就来了，则会Name not found。所以要等下面的流加载完毕再输入卡口数据：往nc 8888端口发车牌号
      }

      override def map2(value: String): String = {
        val splits = value.split(" ")
        map.put(splits(0), splits(1))
        value + " 加载完毕"
      }
    }).print
    env.execute




  }

}
