package com.lisz.stream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._ // 为了flink的隐式转换

object WordCount {
  def main(args: Array[String]): Unit = {
    /**
     * createLocalEnvironment 创建一个本地执行的环境 local
     * createLocalEnvironmentWithWebUI 创建了一个本地执行环境，同时还开起了Web UI的8081端口
     * getExecutionEnvironment 根据执行环境创建上下文，比如local、cluster
     *
     * senv.socketTextStream("hadoop-02", 8888).flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1).print
     * senv.execute("first shell job") // 执行这一句之前如果没有开启：nc -lk 8888,则会报错
     */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 指定并行度：有多少个线程来处理. 数据量很小但是线程很多的话，可能线程启动的时间比数据处理时间还长，适得其反。如果并行度设置为1，则每个算子只会启动一个线程来处理数据
    env.setParallelism(1)
    // Datastream: 一组相同类型的元素组成的数据流, 注意⚠️：要在hadoop-01上启动一个socket：`nc -lk 8888`
    // 如果数据源socket，则initStream的并行度只能是1
    val initStream:DataStream[String] = env.socketTextStream("hadoop-01", 8888)
    val wordStream:DataStream[String] = initStream.flatMap(_.split("\\s+")).setParallelism(2)
    val pairStream = wordStream.map((_, 1)).setParallelism(2)
    val keyByStream = pairStream.keyBy(0) // 按照第一个位置为key, 而不是_后面的标量1
    val restStream = keyByStream.sum(1) // 累加第二个位置，把各个1都加起来
    restStream.print

    env.execute("First flink job")

    /**
     * 6> (lisz,1)
     * 5> (hello,1)
     * 13> (flink,1)
     * 5> (hello,2)
     * 6> (lisz,2)
     * 13> (flink,2)
     * 后面的数字会累加
     * 前面的数字代表是哪一个线程处理的，相同的数据（key）一定由某一个特定的thread处理
     * 每一次在nc那里输入一行，只会打印value有变化的keys，所以后打印出来的可能跟前面的重复
     */
  }

}
