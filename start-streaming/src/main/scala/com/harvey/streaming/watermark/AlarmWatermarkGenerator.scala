package com.harvey.streaming.watermark

import com.rdf.dc.protobuf.DFSSInfoProtos
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

class AlarmWatermarkGenerator(lateThreshold: Long) extends AssignerWithPeriodicWatermarks[DFSSInfoProtos.DFSSInfo]{
  var currentMaxTimestamp: Long = _

  override def extractTimestamp(element: DFSSInfoProtos.DFSSInfo, previousElementTimestamp: Long): Long = {
    val timestamp = element.getAlarm.getTimeStamp * 1000
    currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
    timestamp
  }

  override def getCurrentWatermark: Watermark = new Watermark(currentMaxTimestamp - lateThreshold)
}
