package com.harvey.streaming.spark

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object NetworkWordCount {
  def main(args: Array[String]) {
    //    if (args.length < 2) {
    //      System.err.println("Usage: NetworkWordCount <hostname> <port>")
    //      System.exit(1)
    //    }
    //$ nc -lk 9999
    val host = "localhost"
    val port = 9999

    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.socketTextStream(host, port, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)
    words.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
