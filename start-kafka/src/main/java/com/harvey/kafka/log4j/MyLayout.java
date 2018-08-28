package com.harvey.kafka.log4j;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.LoggingEvent;

//import com.ihandy.myCard.utils.DesUtils;


public class MyLayout extends PatternLayout {
	
	/** base64加密器 */
	private static sun.misc.BASE64Encoder encoder = new sun.misc.BASE64Encoder();
	
	
	//private static DesUtils desUtil =new DesUtils("11122");
	

	
	/** 相关敏感信息的正则表达式 */
	private static final Pattern[] patterns = new Pattern[] {
			//Pattern.compile("[0-9]{15}|[0-9]{17}[0-9xX]"),
			// 身份证校验
			Pattern.compile("(?:1[1-5]|2[1-3]|3[1-7]|4[1-6]|5[0-4]|6[1-5]|71|81|82|91)[0-9]{4}(?:19|20)[0-9]{2}(?:0[1-9]|1[0-2])(?:[012][1-9]|30|31)[0-9]{3}[0-9xX]"),
			
			// 手机校验
			Pattern.compile("1[354789][0-9]{9}"),  
			
	};

	@Override
	public String format(LoggingEvent event) {
		String result = super.format(event);
		
		for (Pattern pattern : patterns) {
			Matcher mch = pattern.matcher(result);
			while (mch.find()) {
				String privateData = mch.group(0);
				result = result.replace(privateData, "{"); // + desUtil.encryptStr(privateData) + "}");
			}
		}
		
		return result;
	}
	
	public static void main(String[] args) {
//		System.out.println(desUtil.decryptStr("cso0NrGYIn+7gieqNnpZiA=="));
	}
}

