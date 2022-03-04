package com.lisz.stream.transformation2

/*
  将上游的所有数据全部发送到下游的第一个分区中。比如上游有3个分区，下游即使有2个，则有一个就没又数据，浪费了。一般用于测试，
  可以先执行一把global再打印
 */
object GlobalTest {
  def main(args: Array[String]): Unit = {

  }
}
