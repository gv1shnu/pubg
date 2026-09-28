package telemetry;

import java.time.Duration;
import java.sql.Timestamp;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public class TelemetryJob {
    static final OutputTag<String> REJECTED=new OutputTag<String>("rejected"){};
    static final OutputTag<Event> LATE=new OutputTag<Event>("excessively-late"){};
    static String env(String key,String fallback) {return System.getenv().getOrDefault(key,fallback);}
    public static class Validate extends ProcessFunction<Wire,Event> {
        private transient Counter rejected, accepted;
        public void open(Configuration config) {
            rejected=getRuntimeContext().getMetricGroup().counter("rejected_events");
            accepted=getRuntimeContext().getMetricGroup().counter("validated_events");
        }
        public void processElement(Wire w,Context ctx,Collector<Event> out) throws Exception {
            Event event;
            try {event=Event.parse(w);}
            catch(Exception ex) {
                rejected.inc(); var n=new ObjectMapper().createObjectNode();
                n.put("namespace",w.namespace.matches("[a-zA-Z0-9_-]{1,64}")?w.namespace:"invalid");n.put("topic",w.topic);n.put("partition",w.partition);n.put("offset",w.offset);
                n.put("reason",ex.getMessage()==null?"invalid event":ex.getMessage());n.put("body",w.body);n.put("processed_at",System.currentTimeMillis());
                ctx.output(REJECTED,n.toString()); return;
            }
            accepted.inc(); out.collect(event);
        }
    }
    public static class Dedupe extends KeyedProcessFunction<String,Event,Event> {
        private transient ValueState<Boolean> seen;
        private transient Counter duplicates, lateCount;
        public void open(Configuration config) {
            var descriptor=new ValueStateDescriptor<Boolean>("seen-event",Boolean.class);
            descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Duration.ofHours(24)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired).build());
            seen=getRuntimeContext().getState(descriptor);
            duplicates=getRuntimeContext().getMetricGroup().counter("duplicate_events");
            lateCount=getRuntimeContext().getMetricGroup().counter("late_events");
        }
        public void processElement(Event e,Context ctx,Collector<Event> out) throws Exception {
            if(Boolean.TRUE.equals(seen.value())) {duplicates.inc(); return;}
            seen.update(true);
            long watermark=ctx.timerService().currentWatermark();
            e.late=watermark!=Long.MIN_VALUE && e.timestamp<watermark-Long.parseLong(env("ALLOWED_LATENESS_MS","60000"));
            e.processed=System.currentTimeMillis();
            if(e.late) {lateCount.inc();ctx.output(LATE,e);}
            out.collect(e); // Late facts persist safely, excluded by the curated view until reconciliation.
        }
    }
    public static class WindowCount extends ProcessWindowFunction<Event,String,String,TimeWindow> {
        public void process(String key,Context ctx,Iterable<Event> values,Collector<String> out) throws Exception {
            Event first=null;long count=0;for(Event e:values){first=e;count++;}
            if(first!=null) {
                var n=new ObjectMapper().createObjectNode();n.put("namespace",first.namespace);n.put("run_id",first.run);n.put("server_id",first.server);n.put("window_end",ctx.window().getEnd());n.put("sample_count",count);out.collect(n.toString());
            }
        }
    }
    static com.fasterxml.jackson.databind.JsonNode json(String value) {
        try { return new ObjectMapper().readTree(value); }
        catch (java.io.IOException error) { throw new IllegalArgumentException("Invalid internal JSON",error); }
    }
    static JdbcConnectionOptions connection() {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl(env("JDBC_URL","jdbc:postgresql://postgres:5432/telemetry"))
            .withDriverName("org.postgresql.Driver").withUsername("stream_writer").withPassword(System.getenv("WRITER_PASSWORD")).build();
    }
    static JdbcExecutionOptions execution() {return JdbcExecutionOptions.builder().withBatchSize(100).withBatchIntervalMs(500).withMaxRetries(5).build();}
    static KafkaSink<String> auditSink(String topic) {
        return KafkaSink.<String>builder().setBootstrapServers(env("KAFKA_BOOTSTRAP","kafka:9092"))
            .setRecordSerializer(KafkaRecordSerializationSchema.builder().setTopic(topic).setValueSerializationSchema(new SimpleStringSchema()).build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build();
    }
    public static void main(String[] args) throws Exception {
        var env=StreamExecutionEnvironment.getExecutionEnvironment();env.setParallelism(1);
        env.enableCheckpointing(10000);env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setCheckpointStorage("file:///checkpoints");
        env.getCheckpointConfig().enableExternalizedCheckpoints(org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10,10000));
        var source=KafkaSource.<Wire>builder().setBootstrapServers(env("KAFKA_BOOTSTRAP","kafka:9092"))
            .setTopics("match.v1","gameplay.v1","session.v1","server.v1").setGroupId("battleground-v1")
            .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
            .setProperty("commit.offsets.on.checkpoint","true").setDeserializer(new Wire.Decoder()).build();
        var validated=env.fromSource(source,WatermarkStrategy.<Wire>forBoundedOutOfOrderness(Duration.ofMillis(Long.parseLong(env("OUT_OF_ORDER_MS","10000"))))
            .withTimestampAssigner((w,t)->w.eventTimestamp).withIdleness(Duration.ofSeconds(15)),"Kafka records").uid("kafka-v1")
            .process(new Validate()).name("Validate contract").uid("validate-v1");
        var accepted=validated
            .keyBy(Event::key).process(new Dedupe()).name("Deduplicate and classify lateness").uid("dedupe-v1");
        accepted.addSink(JdbcSink.sink("INSERT INTO raw.events(namespace,run_id,event_id,envelope,topic,partition_id,kafka_offset,ingested_at,processed_at,excessively_late) VALUES (?,?,?,?::jsonb,?,?,?,?,?,?) ON CONFLICT(namespace,run_id,event_id) DO NOTHING",
            (ps,e)->{ps.setString(1,e.namespace);ps.setString(2,e.run);ps.setString(3,e.id);ps.setString(4,e.body);ps.setString(5,e.topic);ps.setInt(6,e.partition);ps.setLong(7,e.offset);ps.setTimestamp(8,new Timestamp(e.ingested));ps.setTimestamp(9,new Timestamp(e.processed));ps.setBoolean(10,e.late);},execution(),connection()))
            .name("Canonical facts").uid("canonical-jdbc-v1");
        var rejected=validated.getSideOutput(REJECTED);
        rejected.sinkTo(auditSink("dead-letter.v1")).uid("dlq-kafka-v1");
        rejected.addSink(JdbcSink.sink("INSERT INTO ops.rejections VALUES (?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",(ps,s)->{
            var n=json(s);ps.setString(1,n.path("namespace").asText());ps.setString(2,n.path("topic").asText());ps.setInt(3,n.path("partition").asInt());ps.setLong(4,n.path("offset").asLong());ps.setString(5,n.path("reason").asText());ps.setString(6,n.path("body").asText());ps.setTimestamp(7,new Timestamp(n.path("processed_at").asLong()));
        },execution(),connection())).uid("dlq-jdbc-v1");
        accepted.getSideOutput(LATE).map(e->{var n=new ObjectMapper().createObjectNode();n.put("namespace",e.namespace);n.put("topic",e.topic);n.put("partition",e.partition);n.put("offset",e.offset);n.put("reason","older than watermark minus allowed lateness");n.set("event",new ObjectMapper().readTree(e.body));return n.toString();}).returns(String.class)
            .sinkTo(auditSink("excessively-late.v1")).uid("late-kafka-v1");
        accepted.filter(e->e.type.equals("server_sample")&&!e.late).keyBy(e->e.namespace+"/"+e.run+"/"+e.server)
            .window(TumblingEventTimeWindows.of(Time.seconds(10))).allowedLateness(Time.seconds(60))
            .process(new WindowCount()).name("Event-time server window observations").uid("server-windows-v1")
            .addSink(JdbcSink.sink("INSERT INTO ops.window_observations VALUES (?,?,?, ?,?) ON CONFLICT(namespace,run_id,server_id,window_end) DO UPDATE SET sample_count=greatest(ops.window_observations.sample_count,excluded.sample_count)",
                (ps,s)->{var n=json(s);ps.setString(1,n.path("namespace").asText());ps.setString(2,n.path("run_id").asText());ps.setString(3,n.path("server_id").asText());ps.setTimestamp(4,new Timestamp(n.path("window_end").asLong()));ps.setLong(5,n.path("sample_count").asLong());},execution(),connection())).uid("window-jdbc-v1");
        env.execute("Battleground Telemetry v1");
    }
}
