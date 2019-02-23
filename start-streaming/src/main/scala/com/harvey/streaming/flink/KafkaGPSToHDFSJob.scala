package com.harvey.streaming.flink

import java.time.ZoneId
import java.util.Properties

import com.rdf.dc.flink.GPSDeserializationSchema
import com.rdf.dc.json.RdfGPS
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.fs.StringWriter
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object KafkaGPSToHDFSJob {
  val rootGpsPath = "hdfs://192.168.5.102:8020/ods/flink/gps"

  def main(args: Array[String]): Unit = {
    //System.setProperty("HADOOP_USER_NAME", "root")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //env.registerTypeWithKryoSerializer(classOf[RdfGPS], classOf[GPSDeserializationSchema])
    env.enableCheckpointing(60000)

    val properties = new Properties
    properties.setProperty("bootstrap.servers", "192.168.5.103:9092,192.168.5.104:9092,192.168.5.105:9092")
    properties.setProperty("group.id", "group-flink-hdfs-test")

    val consumer: FlinkKafkaConsumer[RdfGPS] =
      new FlinkKafkaConsumer[RdfGPS]("json-gps-v3", new GPSDeserializationSchema(true), properties)

    val sink = new BucketingSink[String](rootGpsPath)
    sink.setBucketer(new DateTimeBucketer[String]("yyyyMMddHH", ZoneId.of("Asia/Shanghai")))
    sink.setWriter(new StringWriter[String]())
    sink.setBatchSize(1024 * 1024 * 100)
    sink.setBatchRolloverInterval(300 * 1000)

    env.addSource(consumer).map(m => m.toString).addSink(sink)
    env.execute("KafkaGPSToHDFSJob")

    //    val input = env.addSource(consumer)
    //      .map(f => ((f.getProcessKey, f), 1))
    //      .keyBy(_._1._1)
    //      .timeWindow(Time.seconds(20))
    //      .sum(1)
    //      .map(m => m._1._2)
    //    val bucketAssigner = new DateTimeBucketAssigner[RdfGPS]("yyyy-MM-dd--HH", ZoneId.of("Asia/Shanghai"))
    //    val sink: StreamingFileSink[RdfGPS] = StreamingFileSink
    //      .forBulkFormat(new Path(rootGpsPath), ParquetAvroWriters.forReflectRecord(classOf[RdfGPS]))
    //      .withBucketAssigner(bucketAssigner)
    //      //.withBucketCheckInterval(60 * 1000)
    //      .build()
    //    input.addSink(sink).name("hdfs sink")
    //    env.execute("KafkaToHDFSJob")
  }
}
