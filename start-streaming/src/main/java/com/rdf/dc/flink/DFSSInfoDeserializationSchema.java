package com.rdf.dc.flink;

import com.google.protobuf.ExtensionRegistry;
import com.googlecode.protobuf.format.JsonFormat;
import com.rdf.dc.protobuf.DFSSInfoProtos;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;


public class DFSSInfoDeserializationSchema implements KeyedDeserializationSchema<DFSSInfoProtos.DFSSInfo> {
    private static final long serialVersionUID = 1509391548173891955L;

    private final boolean includeMetadata;
    private String encoding = "UTF8";

    public DFSSInfoDeserializationSchema(boolean includeMetadata) {
        this.includeMetadata = includeMetadata;
    }

    @Override
    public DFSSInfoProtos.DFSSInfo deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) {
        JsonFormat jsonFormat = new JsonFormat();
        DFSSInfoProtos.DFSSInfo dfssInfo = null;
        if (message != null) {
            try {
                String json = new String(message, this.encoding);
                // protobuf JsonFormat can't handle the value "null" of list type json key
                json = json.replaceAll("\"StateList\"\\s*:\\s*null", "\"StateList\":[]");
                DFSSInfoProtos.DFSSInfo.Builder builder = DFSSInfoProtos.DFSSInfo.newBuilder();
                jsonFormat.merge(json, ExtensionRegistry.getEmptyRegistry(), builder);
                if (includeMetadata) {
                    DFSSInfoProtos.Metadata.Builder metadataBuilder = DFSSInfoProtos.Metadata.newBuilder();
                    metadataBuilder.setTopic(topic);
                    metadataBuilder.setPartition(partition);
                    metadataBuilder.setOffset(offset);
                    builder.setMetadata(metadataBuilder.build());
                }
                dfssInfo = builder.build();

//                String template = "%s$COLUMN_DELIMITED%s$COLUMN_DELIMITED%d$COLUMN_DELIMITED%d$COLUMN_DELIMITED%d$COLUMN_DELIMITED%d$COLUMN_DELIMITED%s$COLUMN_DELIMITED%s$COLUMN_DELIMITED%d$COLUMN_DELIMITED%d$COLUMN_DELIMITED%s$COLUMN_DELIMITED%s$COLUMN_DELIMITED%f$COLUMN_DELIMITED%f$COLUMN_DELIMITED%f$COLUMN_DELIMITED%f$COLUMN_DELIMITED%s$COLUMN_DELIMITED%b$COLUMN_DELIMITED%d$COLUMN_DELIMITED%d$COLUMN_DELIMITED%d$COLUMN_DELIMITED%f$COLUMN_DELIMITED%f$COLUMN_DELIMITED%f$COLUMN_DELIMITED%d$COLUMN_DELIMITED%d$COLUMN_DELIMITED%d$COLUMN_DELIMITED%d$COLUMN_DELIMITED%d$COLUMN_DELIMITED%d$COLUMN_DELIMITED%d$COLUMN_DELIMITED%d$COLUMN_DELIMITED%s$COLUMN_DELIMITED%d$COLUMN_DELIMITED%d$COLUMN_DELIMITED%s$COLUMN_DELIMITED%d$COLUMN_DELIMITED%d$COLUMN_DELIMITED%s";
//                DFSSInfoProtos.DFSSInfo m = dfssInfo;
//                DFSSInfoProtos.Alarm alarm = m.getAlarm();
//                DFSSInfoProtos.Position position = alarm.getPosition();
//
//                String row = String.format(template,m.getProcessKey(), m.getDevID(), m.getDevItnlID(), m.getVehicleItnlID(), m.getDriverItnlID(),
//                        m.getDepID(), m.getTimeZone(), m.getMediaPath(), 0, alarm.getLevel(), "", "",
//                        alarm.getSpeed(), alarm.getVuarSpeed(), alarm.getOriGPSSpeed(), alarm.getGsensorSpeed(), position.getAddress(),
//                        position.getGPSValid(), position.getStars(), position.getACC(), position.getGSMSig(), position.getLat(), position.getLon(),
//                        position.getAngle(), 0, 0, alarm.getAlertTypeID(), 0, 0, 0, 0, 0, "", -1, 0, "", 0, 0, "AI"
//                ).replace("$COLUMN_DELIMITED", "\t");
//                System.out.println(row);

            } catch (Exception ioe) {
                throw new RuntimeException("JsonFormat parse failed: " + ioe.getMessage());
            }
        }
        return dfssInfo;
    }

    @Override
    public boolean isEndOfStream(DFSSInfoProtos.DFSSInfo dfssInfo) {
        return false;
    }

    @Override
    public TypeInformation<DFSSInfoProtos.DFSSInfo> getProducedType() {
        return getForClass(DFSSInfoProtos.DFSSInfo.class);
    }
}
