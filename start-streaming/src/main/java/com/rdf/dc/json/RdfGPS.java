package com.rdf.dc.json;


import com.rdf.dc.util.DateUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Date;

public class RdfGPS {
    private static final String COLUMN_DELIMITED = "\t";

    @JsonProperty("processKey")
    private String processKey;

    @JsonProperty("devId")
    private String devId;

    @JsonProperty("devItnlID")
    private int devItnlID;

    @JsonProperty("vehicleItnlID")
    private int vehicleItnlID;

    @JsonProperty("driverItnlID")
    private int driverItnlID;

    @JsonProperty("simNo")
    private String simNo;

    @JsonProperty("plateNo")
    private String plateNo;

    @JsonProperty("depId")
    private int depId;

    @JsonProperty("timezone")
    private String timezone;

    @JsonProperty("time")
    private long sendTime;

    @JsonProperty("mileage")
    private int mileage;

    @JsonProperty("tireTemp")
    private String tireTemp;

    @JsonProperty("tirePressure")
    private String tirePressure;

    @JsonProperty("brake")
    private int brake;

    @JsonProperty("acc")
    private int acc;

    @JsonProperty("gsmSig")
    private int gsmSig;

    @JsonProperty("speed")
    private float speed;

    @JsonProperty("vuarSpeed")
    private float vuarSpeed;

    @JsonProperty("oriGPSSpeed")
    private float oriGPSSpeed;

    @JsonProperty("gsensorSpeed")
    private float gsensorSpeed;

    @JsonProperty("address")
    private String address;

    @JsonProperty("province")
    private String province;

    @JsonProperty("city")
    private String city;

    @JsonProperty("gpsValid")
    private boolean gpsValid;

    @JsonProperty("stars")
    private int stars;

    @JsonProperty("lat")
    private float lat;

    @JsonProperty("lon")
    private float lon;

    @JsonProperty("alt")
    private float alt;

    @JsonProperty("angle")
    private float angle;

    private String topic;
    private int partition;
    private long offset;

    public String getProcessKey() {
        return processKey;
    }

    public String getDevId() {
        return devId;
    }

    public int getDevItnlID() {
        return devItnlID;
    }

    public int getVehicleItnlID() {
        return vehicleItnlID;
    }

    public int getDriverItnlID() {
        return driverItnlID;
    }

    public String getSimNo() {
        return simNo;
    }

    public String getPlateNo() {
        return plateNo;
    }

    public int getDepId() {
        return depId;
    }

    public String getTimezone() {
        return timezone;
    }

    public long getSendTime() {
        return sendTime;
    }

    public int getMileage() {
        return mileage;
    }

    public String getTireTemp() {
        return tireTemp;
    }

    public String getTirePressure() {
        return tirePressure;
    }

    public int getBrake() {
        return brake;
    }

    public int getAcc() {
        return acc;
    }

    public int getGsmSig() {
        return gsmSig;
    }

    public float getSpeed() {
        return speed;
    }

    public float getGsensorSpeed() {
        return gsensorSpeed;
    }

    public float getOriGPSSpeed() {
        return oriGPSSpeed;
    }

    public float getVuarSpeed() {
        return vuarSpeed;
    }

    public String getAddress() {
        return address;
    }

    public String getProvince() {
        return province;
    }

    public String getCity() {
        return city;
    }

    public boolean isGpsValid() {
        return gpsValid;
    }

    public int getStars() {
        return stars;
    }

    public float getLat() {
        return lat;
    }

    public float getLon() {
        return lon;
    }

    public float getAlt() {
        return alt;
    }

    public float getAngle() {
        return angle;
    }


    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

//    public String toString() {
//        return String.format("processKey:%s, devId:%s, devItnlID:%d, vehicleItnlID:%d, driverItnlID:%d, simNo:%s, plateNo:%s, depId:%d, timezone:%s, time:%d, mileage:%d, tireTemp:%s, tirePressure:%s, brake:%d, acc:%d, gsmSig:%d, speed:%f, vuarSpeed:%f, oriGPSSpeed:%f, gsensorSpeed:%f, address:%s, province:%s, city:%s, gpsValid:%b, stars:%d, lat:%f, lon:%f, alt:%f, angle:%f, topic:%s, partition:%d, offset:%d",
//                processKey, devId, devItnlID, vehicleItnlID, driverItnlID, simNo, plateNo, depId, timezone, timestamp,
//                mileage, tireTemp, tirePressure, brake, acc, gsmSig, speed, vuarSpeed, oriGPSSpeed, gsensorSpeed,
//                address, province, city, gpsValid, stars, lat, lon, alt, angle, topic, partition, offset);
//    }

    public String toString() {
        String template = "%s${COLUMN_DELIMITED}%s${COLUMN_DELIMITED}%d${COLUMN_DELIMITED}%d${COLUMN_DELIMITED}%d${COLUMN_DELIMITED}%s${COLUMN_DELIMITED}%s${COLUMN_DELIMITED}%d${COLUMN_DELIMITED}%s${COLUMN_DELIMITED}%f${COLUMN_DELIMITED}%s${COLUMN_DELIMITED}%s${COLUMN_DELIMITED}%d${COLUMN_DELIMITED}%d${COLUMN_DELIMITED}%d${COLUMN_DELIMITED}%f${COLUMN_DELIMITED}%f${COLUMN_DELIMITED}%f${COLUMN_DELIMITED}%f${COLUMN_DELIMITED}%s${COLUMN_DELIMITED}%d${COLUMN_DELIMITED}%d${COLUMN_DELIMITED}%f${COLUMN_DELIMITED}%f${COLUMN_DELIMITED}%f${COLUMN_DELIMITED}%d${COLUMN_DELIMITED}%f${COLUMN_DELIMITED}%f${COLUMN_DELIMITED}%s${COLUMN_DELIMITED}%s${COLUMN_DELIMITED}%s${COLUMN_DELIMITED}%d";
        template = String.format(template, processKey, devId, devItnlID, vehicleItnlID, driverItnlID, simNo, plateNo, depId, timezone, (float) mileage, tireTemp, tirePressure, brake, acc, gsmSig, speed, vuarSpeed, oriGPSSpeed, gsensorSpeed, address, gpsValid ? 1 : 0, 0, lat, lon, alt, 0, 0.00, 0.00, DateUtils.dateToString(new Date()), DateUtils.dateToString(new Date()), "AI", sendTime)
                .replace("${COLUMN_DELIMITED}", COLUMN_DELIMITED);
        return template;
    }

    public static void main(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        RdfGPS gps = mapper.readValue("{\"processKey\":\"172.20.0.1:40792:5929|4\",\"devId\":\"18\",\"devItnlID\":3018,\"vehicleItnlID\":3018,\"driverItnlID\":0,\"simNo\":\"53000031111113018\",\"plateNo\":\"测5300003111111129\",\"depId\":21,\"timezone\":\"+8\",\"time\":1550807964,\"mileage\":0,\"tireTemp\":\"\",\"tirePressure\":\"\",\"brake\":0,\"acc\":1,\"gsmSig\":1,\"speed\":60,\"vuarSpeed\":0,\"oriGPSSpeed\":0,\"gsensorSpeed\":0,\"address\":\"\",\"province\":\"\",\"city\":\"\",\"gpsValid\":true,\"stars\":5,\"lat\":31.282408,\"lon\":121.542206,\"alt\":0,\"angle\":0}", RdfGPS.class);
        System.out.println(gps.toString());
    }
}


