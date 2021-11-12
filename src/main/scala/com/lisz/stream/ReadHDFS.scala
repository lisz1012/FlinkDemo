package com.lisz.stream

import org.apache.flink.api.common.io.FilePathFilter
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
/**
 * Flume->hdfs->flink实时ETL ->入仓
 * 监控HDFS or 本地目录中文件内容的变化（每隔100ms），一旦HDFS中的文件有追加/新建，则打印其中内容
 */
object ReadHDFS {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val filePath = "hdfs://mycluster/flink/data/"
    val format = new TextInputFormat(new Path(filePath))
    format.setFilesFilter(FilePathFilter.createDefaultFilter)
    val typeInfo = BasicTypeInfo.STRING_TYPE_INFO
    format.setCharsetName("UTF-8")

    val stream = env.readFile(format, filePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 100)
    stream.print

    env.execute
  }

}
