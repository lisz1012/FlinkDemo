package com.lisz.stream.transformation2

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
/*
shuffle增大分区数。此时最好用shuffle或者rebalance。而rescale分发得不会特别均匀
shuffle - 随机，数据量大的时候也均匀
rebalance - 轮询，均匀
这两个算子能尽可能保证在数据量比较小的情况下，把数据比较均匀的发送到下游。常用的场景是发生了数据倾斜之后，可以将数据均衡一下，
或者当需要提高并行读的时候.数据量大的时候shuffle也很均匀，但是还是没有rebalance的轮询均匀，参考Kafka无key时的默认分区策略
 */
object ShuffleTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.generateSequence(1, 10).setParallelism(1)
    println(stream.getParallelism)
    stream.shuffle.print  // stream.shuffle之后得到一个新的data stream 默认有16个分区，因为当前mac有16个核，然后把records随机分配到各个分区
    env.execute
  }
}
