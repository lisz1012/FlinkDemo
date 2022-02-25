package com.lisz.stream.transformation2


import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala.{ConnectedStreams, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
 * connect算子也是将这两个数据流合并，优点：被合并的两个数据流中的数据类型可以不一致；union元素类型必须一致。connect合并之后的流还不能直接打印
 */
/**
 * 现在有一个配置文件，存储了车牌号和车主的真实姓名
 * 通过数据流中的车牌号实时匹配出车主的姓名（注意：配置文件可能实时改变）
 * 配置文件实时改变，读的时候则需要 readFile，而不是readTextFile，因为前者可以指定一直读取，也就是监控
 */
object CoFlatMap {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.socketTextStream("hadoop-01", 8888)
    val ds2 = env.socketTextStream("hadoop-01", 9999)
//    val intStream = ds1.map(_.toInt) // 故意让两者类型不一样
    val connect: ConnectedStreams[String, String] = ds1.connect(ds2)

    // 使用这种遍历ConnectedStreams collect发射的数据类型要一致
    connect.flatMap((i, c:Collector[String])=>{ // Collector就是个发射器，往下游发射数据
      i.split(" ").foreach(c.collect)
    }, (s, c:Collector[String])=>{
      s.split(" ").foreach(c.collect)
    }).print

    env.execute
  }
}
