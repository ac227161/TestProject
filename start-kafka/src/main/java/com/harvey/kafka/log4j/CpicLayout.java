package com.harvey.kafka.log4j;

import java.util.UUID;

import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.LoggingEvent;

public class CpicLayout extends PatternLayout{

	/** 文件路径 */
	public static String logPath = getLogPath();
	/** 文件唯一标示*/
	public static String fileSign = getSign();
	/** 行号用于排序*/
	public static long lineNumber;
	/**
	 * <b>kafka日志生成规则</b>
	 * <br>系统名称|@#&|ip地址|@#&|文件路径|@#&|文件唯一标示|@#&|行号用于排序|@#&|时间戳(秒)|@#&|消息本体
	 */
	@Override
	public String format(LoggingEvent event) {
		String result = super.format(event);
		result = Log4jConstant.SYSTEM+Log4jConstant.KAFKA_SEPARATORS+Log4jConstant.localIP+
				Log4jConstant.KAFKA_SEPARATORS+logPath+
				Log4jConstant.KAFKA_SEPARATORS+fileSign+
				Log4jConstant.KAFKA_SEPARATORS+(lineNumber++)+
				Log4jConstant.KAFKA_SEPARATORS+(System.currentTimeMillis()/1000)+
				Log4jConstant.KAFKA_SEPARATORS+result;
		return result;
	}
	public static String getSign() {
		return UUID.randomUUID().toString();
	}
	public static String getLogPath() {
		String className = CpicLayout.class.getSimpleName();
		String fileName = className.substring(0, className.indexOf("Layout")).toLowerCase();
		return Log4jConstant.getFilePathPrefix()+fileName+".log";
	}
}
