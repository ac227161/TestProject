package com.harvey.streaming.flink

import java.util.Properties

import com.harvey.streaming.watermark.AlarmWatermarkGenerator
import com.rdf.dc.flink.DFSSInfoDeserializationSchema
import com.rdf.dc.protobuf.DFSSInfoProtos
import com.twitter.chill.protobuf.ProtobufSerializer
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object KafkaConnectorDemo {
  val sourceAlarmList = Array(2, 18, 22, 11, 12, 13, 14)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.registerTypeWithKryoSerializer(classOf[DFSSInfoProtos.DFSSInfo], classOf[ProtobufSerializer])

    val properties = new Properties
    properties.setProperty("bootstrap.servers", "192.168.5.103:9092,192.168.5.104:9092,192.168.5.105:9092")
    properties.setProperty("group.id", "group-flink-test")

    val consumer: FlinkKafkaConsumer[DFSSInfoProtos.DFSSInfo] = new FlinkKafkaConsumer[DFSSInfoProtos.DFSSInfo](
      "json-alarm", new DFSSInfoDeserializationSchema(true), properties)
    consumer.setStartFromGroupOffsets().setCommitOffsetsOnCheckpoints(true)

    val stream = env.addSource(consumer).assignTimestampsAndWatermarks(new AlarmWatermarkGenerator(1200 * 1000))
      //.map(m => ((m.getVehicleItnlID, m.getDevItnlID), 1)).keyBy(_._1)
      .map(m => ((m.getProcessKey, m.toString), 1)).keyBy(_._1._1)
      .timeWindow(Time.seconds(20), Time.seconds(5))
      .sum(1).map(m => m._1._2)


    //    val pattern = Pattern.begin[DFSSInfoProtos.DFSSInfo]("start")
    //      .where(_.getAlarm.getAlertTypeID == 2).times(2).within(Time.seconds(20)) //.subtype().until
    //val patternStream = CEP.pattern(stream, pattern)
    //val result = patternStream.select()
    stream.print()

    env.execute("KafkaConnectorDemo")

  }
}
