package com.rdf.dc.flink;

import com.google.protobuf.ExtensionRegistry;
import com.googlecode.protobuf.format.JsonFormat;
import com.rdf.dc.protobuf.DFSSInfoProtos;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;


public class DFSSInfoDeserializationSchema implements KeyedDeserializationSchema<DFSSInfoProtos.DFSSInfo> {
    private static final long serialVersionUID = 1509391548173891955L;

    private String encoding = "UTF8";

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
                dfssInfo = builder.build();
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
