package com.harvey.streaming.flink


import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.slf4j.LoggerFactory

object SocketWindowWordCount {
  val logger = LoggerFactory.getLogger(SocketWindowWordCount.getClass)

  def main(args: Array[String]): Unit = {
    val host = "localhost"
    val port = 9999

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream(host, port)


    val windowCounts = text
      .flatMap { w => w.split("\\s+") }
      .map { w => WordWithCount(w, 1) }
      .keyBy("word")
      .timeWindow(Time.seconds(5), Time.seconds(1))
      .sum("count")
    windowCounts.print().setParallelism(1)

    env.execute("Socket Window WordCount")
  }

  // Data type for words with count
  case class WordWithCount(word: String, count: Long)
}
