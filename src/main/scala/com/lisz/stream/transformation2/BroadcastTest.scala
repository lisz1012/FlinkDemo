package com.lisz.stream.transformation2

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
/*
  broadcast广播：每个分区都往下游的各个分区发送他所拥有的所有数据，造成大量冗余。它的应用场景是处理数据的时候使用了映射表：根据车牌号
  找车主姓名，车牌号映射会动态改变，当初做的是：readFile + connect，实际上的应用是把映射表放在Kafka里面，这个时候再读的话，就会得到一个dataStream，
  把这个Kafka的数据流往下又的每个分区中都broadcast一份，则下又的每个分区都能收到映射表的信息。细节：如果业务流比映射流启动的还早，则导致
  映射流映射不出任何信息，将来可以通过 Redis + MySQL 来解决这些问题。
 */
object BroadcastTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.generateSequence(1, 10).setParallelism(2)
    stream.writeAsText("./data/stream5").setParallelism(2)  // 保存到本地，2个文件，每个文件（分区或者subTask）只有一半数据
    stream.broadcast.writeAsText("./data/stream6").setParallelism(4)  // 保存到本地，4个文件，每个文件（分区或者subTask）都有所有上游的数据
    env.execute
  }
}
