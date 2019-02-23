package com.rdf.dc.flink;

import com.rdf.dc.json.RdfGPS;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

import java.io.IOException;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

public class GPSDeserializationSchema implements KeyedDeserializationSchema<RdfGPS> {
    private final boolean includeMetadata;
    private ObjectMapper mapper;

    public GPSDeserializationSchema(boolean includeMetadata) {
        this.includeMetadata = includeMetadata;
    }

    @Override
    public RdfGPS deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) throws IOException {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        RdfGPS gps = mapper.readValue(message, RdfGPS.class);
        if (includeMetadata) {
            gps.setTopic(topic);
            gps.setPartition(partition);
            gps.setOffset(offset);
        }
        return gps;
    }

    @Override
    public boolean isEndOfStream(RdfGPS nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RdfGPS> getProducedType() {
        return getForClass(RdfGPS.class);
    }
}
