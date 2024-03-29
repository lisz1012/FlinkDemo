package com.lisz.stream.source

import org.apache.flink.api.common.io.FilePathFilter
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

/**
 * Flume->hdfs->flink实时ETL ->入仓
 * 监控HDFS or 本地目录中文件内容的变化（每隔100ms），一旦HDFS中的文件有追加/新建，则打印其中内容
 * 启动程序后，
 * 执行： hdfs dfs -put ./wc4 /flink/data/  或者
 *       hdfs dfs -appendToFile ./wc4 /flink/data/wc4
 * 就能看到实时的文件改变的结果的打印.
 *
 * Flume->hdfs->flink实时ETL ->入仓 也行
 */
object ReadHDFS {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val filePath = "hdfs://mycluster/flink/data/"  // hdfs-site和core-site两个xml为难要放在resources目录下
    val format = new TextInputFormat(new Path(filePath))
    format.setFilesFilter(FilePathFilter.createDefaultFilter)  // 可选设置，可以把files starting with ".", "_", and "_COPYING_"这些临时文件的变化从监控中过滤掉.
    format.setCharsetName("UTF-8")                             // 可选设置

    val stream = env.readFile(format, filePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 100) // 需要导入隐式转换：import org.apache.flink.api.scala._
    stream.print

    env.execute
  }
}
