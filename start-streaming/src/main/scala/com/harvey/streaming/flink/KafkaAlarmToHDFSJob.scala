package com.harvey.streaming.flink

import java.time.ZoneId
import java.util.{Date, Properties}

import com.rdf.dc.flink.DFSSInfoDeserializationSchema
import com.rdf.dc.protobuf.DFSSInfoProtos.DFSSInfo
import com.rdf.dc.util.DateUtils
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.fs.StringWriter
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object KafkaAlarmToHDFSJob {
  val rootAlarmPath = "hdfs://192.168.5.102:8020/ods/flink/alarm"
  val COLUMN_DELIMITED = "\t"
  val template = s"%s$COLUMN_DELIMITED%s$COLUMN_DELIMITED%d$COLUMN_DELIMITED%d$COLUMN_DELIMITED%d$COLUMN_DELIMITED%d$COLUMN_DELIMITED%s$COLUMN_DELIMITED%s$COLUMN_DELIMITED%d$COLUMN_DELIMITED%d$COLUMN_DELIMITED%d$COLUMN_DELIMITED%s$COLUMN_DELIMITED%s$COLUMN_DELIMITED%f$COLUMN_DELIMITED%f$COLUMN_DELIMITED%f$COLUMN_DELIMITED%f$COLUMN_DELIMITED%s$COLUMN_DELIMITED%b$COLUMN_DELIMITED%d$COLUMN_DELIMITED%d$COLUMN_DELIMITED%d$COLUMN_DELIMITED%f$COLUMN_DELIMITED%f$COLUMN_DELIMITED%f$COLUMN_DELIMITED%d$COLUMN_DELIMITED%d$COLUMN_DELIMITED%d$COLUMN_DELIMITED%d$COLUMN_DELIMITED%d$COLUMN_DELIMITED%d$COLUMN_DELIMITED%d$COLUMN_DELIMITED%s$COLUMN_DELIMITED%d$COLUMN_DELIMITED%d$COLUMN_DELIMITED%s$COLUMN_DELIMITED%d$COLUMN_DELIMITED%d$COLUMN_DELIMITED%s"

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(300 * 1000)

    val properties = new Properties
    properties.setProperty("bootstrap.servers", "192.168.5.103:9092,192.168.5.104:9092,192.168.5.105:9092")
    properties.setProperty("group.id", "group-flink-hdfs-test")

    val consumer: FlinkKafkaConsumer[DFSSInfo] =
      new FlinkKafkaConsumer[DFSSInfo]("json-alarm", new DFSSInfoDeserializationSchema(true), properties)
    consumer.setStartFromLatest()

    val sink = new BucketingSink[String](rootAlarmPath)
    sink.setBucketer(new DateTimeBucketer[String]("yyyyMMddHH", ZoneId.of("Asia/Shanghai")))
    sink.setWriter(new StringWriter[String]())
    sink.setBatchSize(1024 * 1024 * 100)
    sink.setBatchRolloverInterval(60 * 1000)

    env.addSource(consumer).map(m => {
      val alarm = m.getAlarm
      val position = alarm.getPosition
      val datetimeStr = DateUtils.dateToString(new Date())
      val actionTime = alarm.getTimeStamp
      val actionHour = DateUtils.unixToDateTimeStr(actionTime, DateUtils.HOUR_FORMATTER).toLong

      template.format(m.getProcessKey, m.getDevID, m.getDevItnlID, m.getVehicleItnlID, m.getDriverItnlID,
        m.getDepID, m.getTimeZone, m.getMediaPath, 0, alarm.getAlertTypeID, alarm.getLevel, datetimeStr, datetimeStr,
        alarm.getSpeed, alarm.getVuarSpeed, alarm.getOriGPSSpeed, alarm.getGsensorSpeed, position.getAddress,
        position.getGPSValid, position.getStars, position.getACC, position.getGSMSig, position.getLat, position.getLon,
        position.getAngle, actionTime, actionHour, 0, 0, 0, 0, 0, "", -1, 0, "", 0, 0, "AI"
      )
    }).addSink(sink)
    env.execute("KafkaAlarmToHDFSJob")
  }
}
