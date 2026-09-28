package telemetry;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class Wire implements Serializable {
    public String namespace="live", topic, body;
    public int partition;
    public long offset, ingested, eventTimestamp;
    public Wire() {}
    public static class Decoder implements KafkaRecordDeserializationSchema<Wire> {
        public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Wire> out) {
            Wire w=new Wire(); w.topic=record.topic(); w.partition=record.partition(); w.offset=record.offset();
            w.body=record.value()==null ? "null" : new String(record.value(),StandardCharsets.UTF_8);
            var header=record.headers().lastHeader("namespace");
            if(header!=null && header.value()!=null) w.namespace=new String(header.value(),StandardCharsets.UTF_8);
            w.ingested=System.currentTimeMillis(); w.eventTimestamp=Long.MIN_VALUE;
            try {
                var node=new com.fasterxml.jackson.databind.ObjectMapper().readTree(w.body);
                long timestamp=java.time.Instant.parse(node.path("event_time").asText()).toEpochMilli();
                if(timestamp<=w.ingested+30000) w.eventTimestamp=timestamp;
            } catch(Exception ignored) { /* Validation emits the full rejection downstream. */ }
            out.collect(w);
        }
        public TypeInformation<Wire> getProducedType() { return TypeInformation.of(Wire.class); }
    }
}
