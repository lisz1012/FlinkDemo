package com.lisz.stream.transformation

import org.apache.flink.streaming.api.scala.{ConnectedStreams, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

/**
 * 现在有一个配置文件，存储了车牌号和车主的真实姓名
 * 通过数据流中的车牌号实时匹配出车主的姓名（注意：配置文件可能实时改变）
 * 配置文件实时改变，读的时候则需要 readFile，而不是readTextFile，因为前者可以指定一直读取，也就是监控
 */
object CoFlatMap {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream1 = env.socketTextStream("hadoop-01", 8888)
    val stream2 = env.socketTextStream("hadoop-01", 9999)
    //val intStream = stream1.map(_.toInt)
    // val connect:ConnectedStreams[Int, String] = intStream.connect(stream2)
    // Collector（下面的那个c）是发射器，往下游发射数据
    // 使用这种遍历ConnectedStreams collect发射的数据类型要一致
//    connect.flatMap((x, c) => {
//      c.collect(x)
//    }, (x, c) => {
//      x.split(" ").foreach(y => c.collect(y)) // 把每个单词给发射出去
//    })
    val connect = stream1.connect(stream2)
    connect.flatMap((x,c:Collector[String])=>{
      x.split(" ").foreach(w => c.collect(w))
    }, (y,c:Collector[String])=>{
      y.split(" ").foreach(d => c.collect(d))
    }).print
    env.execute
  }
}
