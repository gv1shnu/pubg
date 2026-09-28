package telemetry;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class EventTest {
    Wire wire(String body) { Wire w=new Wire();w.body=body;w.topic="server.v1";w.ingested=System.currentTimeMillis();return w; }
    String valid() { return """
      {"event_id":"e1","event_type":"server_sample","schema_version":1,"run_id":"r1","producer_id":"server",
      "event_time":"2026-01-01T00:00:00Z","emitted_at":"2026-01-01T00:00:00Z","sequence":1,
      "match_id":"m1","server_id":"s1","simulated":true,"simulation_elapsed_seconds":0,
      "payload":{"cpu_pct":30,"memory_pct":40,"latency_ms":[10,20],"packets_sent":100,"packets_lost":2,
      "tick_ms":12,"target_tick_ms":16.667,"online_players":1,"incident":false}}
      """; }
    @Test void validAndSourceCoordinates() throws Exception {
        Wire w=wire(valid());w.partition=1;w.offset=42;
        Event e=Event.parse(w);assertEquals("live/r1/e1",e.key());assertEquals(42,e.offset);assertEquals(1,e.partition);
    }
    @Test void rejectsMalformedAndUnsupported() {
        assertThrows(Exception.class,()->Event.parse(wire("{bad")));
        assertThrows(Exception.class,()->Event.parse(wire(valid().replace("\"schema_version\":1","\"schema_version\":2"))));
    }
    @Test void rejectsInconsistentPacketCountsAndWrongTopic() {
        assertThrows(Exception.class,()->Event.parse(wire(valid().replace("\"packets_lost\":2","\"packets_lost\":200"))));
        Wire w=wire(valid());w.topic="gameplay.v1";assertThrows(Exception.class,()->Event.parse(w));
    }
    @Test void replayPreservesIdentityButSeparatesNamespace() throws Exception {
        Wire w=wire(valid());Event original=Event.parse(w);w.namespace="replay-test";Event replay=Event.parse(w);
        assertEquals(original.id,replay.id);assertEquals(original.timestamp,replay.timestamp);assertNotEquals(original.key(),replay.key());
    }
}
