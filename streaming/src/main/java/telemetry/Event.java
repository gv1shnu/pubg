package telemetry;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashSet;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;

public class Event implements Serializable {
    public String namespace, run, id, match, server, type, body, topic;
    public int partition;
    public long offset, ingested, processed, timestamp, emitted, sequence;
    public boolean late;
    public Event() {}
    private static final ObjectMapper JSON=new ObjectMapper();
    private static final JsonSchema SCHEMA=JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012)
        .getSchema(Event.class.getResourceAsStream("/events.v1.schema.json"));
    public static Event parse(Wire w) throws Exception {
        if(!w.namespace.matches("[a-zA-Z0-9_-]{1,64}")) throw new IllegalArgumentException("invalid namespace");
        if(w.body.length()>262144) throw new IllegalArgumentException("event exceeds 256 KiB");
        JsonNode n=JSON.readTree(w.body);
        var errors=SCHEMA.validate(n);
        if(!errors.isEmpty()) throw new IllegalArgumentException("schema validation failed: "+errors.iterator().next().getMessage());
        Event e=new Event(); e.namespace=w.namespace; e.run=n.path("run_id").asText(); e.id=n.path("event_id").asText();
        if(!e.run.matches("[a-zA-Z0-9_.-]{1,128}") || !e.id.matches("[a-zA-Z0-9_.-]{1,128}")) throw new IllegalArgumentException("invalid source identity");
        e.match=n.path("match_id").asText(); e.server=n.path("server_id").asText(); e.type=n.path("event_type").asText();
        String expected=e.type.startsWith("match_")||e.type.equals("zone_changed") ? "match.v1" :
            e.type.equals("server_sample")? "server.v1" : e.type.matches("player_(connected|disconnected|reconnected)")? "session.v1":"gameplay.v1";
        if(!expected.equals(w.topic)) throw new IllegalArgumentException("wrong source topic");
        var p=n.path("payload");
        if(e.type.equals("damage_dealt") && p.path("health_before").asInt()-p.path("damage").asInt()!=p.path("health_after").asInt()) throw new IllegalArgumentException("inconsistent health");
        if(e.type.equals("server_sample")) {
            if(p.path("packets_lost").asLong()>p.path("packets_sent").asLong()) throw new IllegalArgumentException("invalid packet counts");
            for(var v:p.path("latency_ms")) if(v.asDouble()<0) throw new IllegalArgumentException("negative latency");
        }
        if(expected.equals("session.v1") && p.path("connected").asBoolean()==e.type.equals("player_disconnected")) throw new IllegalArgumentException("invalid session transition payload");
        if(e.type.equals("match_finished")) {
            var placements=new HashSet<Integer>(); var players=new HashSet<String>(); var ids=new HashSet<String>();
            int count=p.path("outcomes").size(), kills=0;
            for(var row:p.path("outcomes")) {
                int placement=row.path("placement").asInt();
                if(placement>count || !placements.add(placement) || !players.add(row.path("player_id").asText())) throw new IllegalArgumentException("invalid outcomes");
                kills+=row.path("kills").asInt();
            }
            for(var id:p.path("expected_event_ids")) ids.add(id.asText());
            if(kills!=count-1 || ids.size()!=p.path("expected_gameplay_count").asInt() || ids.size()!=p.path("expected_event_ids").size() || ids.size()!=p.path("gameplay_sequence_max").asInt()) throw new IllegalArgumentException("invalid manifest");
        }
        e.timestamp=Instant.parse(n.path("event_time").asText()).toEpochMilli();
        e.emitted=Instant.parse(n.path("emitted_at").asText()).toEpochMilli();
        if(e.timestamp>w.ingested+30000 || e.emitted>w.ingested+30000) throw new IllegalArgumentException("clock ahead by more than 30s");
        e.sequence=n.path("sequence").asLong(); e.body=w.body; e.topic=w.topic; e.partition=w.partition;
        e.offset=w.offset; e.ingested=w.ingested; e.processed=System.currentTimeMillis();
        return e;
    }
    public String key() { return namespace+"/"+run+"/"+id; }
}
