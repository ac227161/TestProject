package com.harvey.streaming.flink

import java.util.Properties

import com.rdf.dc.flink.DFSSInfoDeserializationSchema
import com.rdf.dc.protobuf.DFSSInfoProtos
import com.twitter.chill.protobuf.ProtobufSerializer
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object KafkaConnectorDemo {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.registerTypeWithKryoSerializer(classOf[DFSSInfoProtos.DFSSInfo], classOf[ProtobufSerializer])

    val properties = new Properties
    properties.setProperty("bootstrap.servers", "192.168.5.103:9092,192.168.5.104:9092,192.168.5.105:9092")
    properties.setProperty("group.id", "group-flink-test")

    val consumer: FlinkKafkaConsumer011[DFSSInfoProtos.DFSSInfo] = new FlinkKafkaConsumer011[DFSSInfoProtos.DFSSInfo](
      "json-gps", new DFSSInfoDeserializationSchema(), properties)
    consumer.setStartFromGroupOffsets()

    val stream = env.addSource(consumer) //.map(m => (m, 1)).keyBy(_._1).timeWindow(Time.seconds(5)).sum(1)
    stream.print()

    env.execute("KafkaConnectorDemo")

  }
}
