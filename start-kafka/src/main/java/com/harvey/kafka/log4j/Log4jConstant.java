package com.harvey.kafka.log4j;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.PatternLayout;

//import com.wisdom.cpic.callInterface.InitLoadData;

public class Log4jConstant {

    /**
     * 系统名称
     */
    public static final String SYSTEM = "集团2013版微信公众平台";
    /**
     * ip地址
     */
    public static String localIP = getLocalIp();
    /**
     * 分隔符
     */
    public static final String KAFKA_SEPARATORS = "|@#&|";
    /**
     * 默认的ip
     */
    public static final String DEFAULT_IP = "10.180.42.6";
    public static final String DEFAULT_FILE_PATH_PREFIX = "/app/app/mic/logs/platform/";

    public static String getSign() {
        return UUID.randomUUID().toString();
    }

    /**
     * 获取配置文件路径
     *
     * @return
     */
    public static String getFilePathPrefix() {
        String filePathPrefix = "";
        try {
            filePathPrefix = DEFAULT_FILE_PATH_PREFIX; //InitLoadData.getSysConfByCode("kafka_file_path_prefix").getParamValue();
        } catch (Exception e) {
            filePathPrefix = DEFAULT_FILE_PATH_PREFIX;
        }
        if (StringUtils.isEmpty(filePathPrefix)) {
            filePathPrefix = DEFAULT_FILE_PATH_PREFIX;
        }
        return filePathPrefix;
    }

    /**
     * 获取服务器IP
     *
     * @return
     */
    public static String getLocalIp() {
        String ip = DEFAULT_IP;
        try {
            InetAddress address = InetAddress.getLocalHost();
            ip = address.getHostAddress();
        } catch (UnknownHostException e) {
        }
        return ip;
    }
}
