"""Four independently scheduled source loops, coordinated by authoritative match state."""
import asyncio
import json
import os
import random
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from confluent_kafka import Producer
from prometheus_client import Counter, Gauge, start_http_server
from simulator.contracts import validate, TOPICS
from simulator.engine import Match

SENT = Counter('producer_delivered', 'Broker-acknowledged deliveries, including duplicate injection', ['source'])
FAIL = Counter('producer_delivery_errors', 'Delivery failures', ['source'])
ACTIVE = Gauge('producer_active', '1 while a run is active')

def now():
    return datetime.now(timezone.utc).isoformat()

def control():
    path = Path('/control/settings.json')
    return json.loads(path.read_text()) if path.exists() else {}

class Source:
    def __init__(self, name, run_id, fault):
        self.name, self.run_id, self.fault = name, run_id, fault
        self.sequence = defaultdict(int)
        self.ids = defaultdict(list)
        self.delayed = []
        self.errors = []
        self.rng = random.Random(int(os.getenv("SEED","42"))+sum((i+1)*ord(c) for i,c in enumerate(name)))
        self.client = Producer({'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP', 'kafka:9092'), 'enable.idempotence': True, 'client.id': name, 'delivery.timeout.ms': 30000})
    def ack(self, error, message):
        if error:
            FAIL.labels(self.name).inc()
            self.errors.append(str(error))
        else:
            SENT.labels(self.name).inc()
    def deliver(self, event, namespace='live'):
        topic = TOPICS[self.name]
        key = event['match_id'] if self.name in ('match', 'gameplay') else event['server_id']
        self.client.produce(topic, key=f'{namespace}:{event["run_id"]}:{key}', value=json.dumps(event), headers={'namespace': namespace}, on_delivery=self.ack)
        self.client.poll(0)
    def emit(self, match, event_type, payload, player=None):
        entity = match['id'] if self.name in ('match', 'gameplay') else match['server']
        self.sequence[entity] += 1
        seq = self.sequence[entity]
        timestamp = now()
        event = validate(dict(event_id=str(uuid.uuid5(uuid.NAMESPACE_URL, f'{self.run_id}/{self.name}/{entity}/{seq}')),
            event_type=event_type, schema_version=1, run_id=self.run_id, producer_id=self.name,
            event_time=timestamp, emitted_at=timestamp, sequence=seq, match_id=match['id'],
            server_id=match['server'], player_id=player, simulated=True,
            simulation_elapsed_seconds=(time.monotonic()-match['start'])*6, payload=payload))
        self.ids[match['id']].append(event['event_id'])
        fault = control().get('scenario', self.fault)
        if self.name == 'gameplay' and fault in ('out-of-order', 'excessively-late') and seq % 19 == 0:
            self.delayed.append((time.monotonic()+(3 if fault == 'out-of-order' else 90), event))
        elif self.name == 'gameplay' and fault == 'finish-first':
            self.delayed.append((float('inf'), event))
        else:
            self.deliver(event)
        if (fault == 'duplicate' and seq % 7 == 0) or self.rng.random()<float(os.getenv('DUPLICATE_PROBABILITY','0')):
            self.deliver(event)
        if fault == 'invalid' and seq == 1:
            bad = dict(event, schema_version=999)
            self.client.produce(TOPICS[self.name], value=json.dumps(bad), on_delivery=self.ack)
        return event
    def flush_delayed(self, all_events=False):
        remaining=[]
        for due, event in self.delayed:
            if all_events or due <= time.monotonic():
                self.deliver(dict(event, emitted_at=now()))
            else:
                remaining.append((due,event))
        self.delayed = remaining
    async def wait(self, seconds):
        until = time.monotonic()+seconds
        while time.monotonic() < until or control().get('paused', False):
            self.client.poll(0)
            self.flush_delayed()
            if self.errors:
                raise RuntimeError(f'{self.name} delivery failed')
            remaining=until-time.monotonic()
            await asyncio.sleep(.05 if remaining <= 0 else min(.05,remaining))

async def run():
    start_http_server(8000)
    count, duration, rate = int(os.getenv('PLAYERS','100')), int(os.getenv('DURATION_SECONDS','240')), float(os.getenv('EVENT_RATE','20'))
    concurrent = int(os.getenv('CONCURRENT_MATCHES','1'))
    if not (10 <= duration <= 600 and 1 <= rate <= 100 and 1 <= concurrent <= 3):
        raise ValueError('load outside demo bounds')
    for name in ('DUPLICATE_PROBABILITY','LATENCY_SPIKE_PROBABILITY','DISCONNECT_BURST_PROBABILITY'):
        if not 0 <= float(os.getenv(name,'0')) <= 1: raise ValueError(f'{name} must be between 0 and 1')
    if rate*duration/concurrent > 5000:
        raise ValueError('Per-match event budget exceeds bounded final manifest size (5000 target events)')
    run_id = os.getenv('RUN_ID') or str(uuid.uuid4())
    seed = int(os.getenv('SEED','42'))
    regions = os.getenv('REGIONS','ap-south,eu-west').split(',')
    fault = os.getenv('SCENARIO','normal')
    sources = {n: Source(n, run_id, fault) for n in TOPICS}
    matches = [dict(id=f'match-{i+1}', server=f'server-{i%2+1}', region=regions[(i%2)%len(regions)], model=Match(seed+i,count), start=time.monotonic(), done=False, finished=False, sessions={}) for i in range(concurrent)]
    ACTIVE.set(1)
    print(json.dumps({'run_id':run_id,'seed':seed,'simulated':True}), flush=True)
    async def coordinator():
        s=sources['match']
        for m in matches:
            s.emit(m,'match_created',dict(players=list(m['model'].alive),region=m['region'],map='Amber Basin',duration_seconds=duration))
            s.emit(m,'match_started',dict(alive=count))
        zones=set()
        while not all(m['finished'] for m in matches):
            for m in matches:
                elapsed=time.monotonic()-m['start']
                phase=min(5,int(elapsed/duration*5)+1)
                if (m['id'],phase) not in zones:
                    s.emit(m,'zone_changed',dict(phase=phase,radius_m=1000/phase))
                    zones.add((m['id'],phase))
                if m['done'] and not m['finished']:
                    ids=sources['gameplay'].ids[m['id']]
                    s.emit(m,'match_finished',dict(outcomes=m['model'].result(),expected_gameplay_count=len(ids),gameplay_sequence_max=len(ids),expected_event_ids=ids.copy()))
                    m['finished']=True
            await s.wait(.5)
        # The final manifest can reach Kafka before delayed gameplay by design.
        for source in sources.values():
            source.flush_delayed(True)
    async def gameplay():
        s=sources['gameplay']
        positions=random.Random(seed+10000)
        next_action={m['id']:0. for m in matches}
        tick=1/max(1,rate/concurrent-4)
        tick_index=0
        while not all(m['done'] for m in matches):
            for m in matches:
                if m['done']: continue
                elapsed=tick_index*tick
                if elapsed >= next_action[m['id']]:
                    for kind,p,payload in m['model'].step(): s.emit(m,kind,payload,p)
                    next_action[m['id']]+=duration/(2*(count-1))
                if len(m['model'].alive)==1:
                    m['done']=True
                else:
                    s.emit(m,'player_position_sample',dict(x=positions.uniform(0,1000),y=positions.uniform(0,1000),alive=len(m['model'].alive)), positions.choice(m['model'].alive))
            tick_index+=1
            await s.wait(tick)
    async def sessions():
        s=sources['session']; tick=0
        # Spread connects over the match; sessions and alive players are different concepts.
        while not all(m['finished'] for m in matches):
            for m in matches:
                players=list(m['model'].outcomes)
                burst=(control().get('scenario',fault)=='disconnect-burst' and 20 <= tick%60 < 30) or s.rng.random()<float(os.getenv('DISCONNECT_BURST_PROBABILITY','0'))
                candidates=[p for p,(_,connected) in m['sessions'].items() if connected] if burst else players
                if not candidates: continue
                p=candidates[tick%len(candidates)]
                existing=m['sessions'].get(p)
                connected=not burst and (existing is None or not existing[1])
                sid=existing[0] if existing else str(uuid.uuid5(uuid.NAMESPACE_URL,run_id+m['id']+p))
                kind='player_connected' if existing is None else ('player_reconnected' if connected else 'player_disconnected')
                s.emit(m,kind,dict(session_id=sid,connected=connected),p)
                m['sessions'][p]=(sid,connected)
            tick+=1
            await s.wait(.5)
        for m in matches:
            for p,(sid,connected) in m['sessions'].items():
                if connected: s.emit(m,'player_disconnected',dict(session_id=sid,connected=False),p)
    async def servers():
        s=sources['server']; rng=random.Random(seed+20000); tick=0
        while not all(m['finished'] for m in matches):
            for server in sorted({m['server'] for m in matches}):
                m=next(m for m in matches if m['server']==server)
                online=sum(sum(int(c) for _,c in x['sessions'].values()) for x in matches if x['server']==server)
                spike=(control().get('scenario',fault)=='latency-spike' and 10 <= tick%40 < 25) or rng.random()<float(os.getenv('LATENCY_SPIKE_PROBABILITY','0'))
                s.emit(m,'server_sample',dict(cpu_pct=min(99,15+online*.4+(35 if spike else 0)),memory_pct=30+online*.1,
                    latency_ms=[round(rng.uniform(20,40)+(180 if spike else 0),2) for _ in range(20)],packets_sent=1000+online*10,
                    packets_lost=60 if spike else 2,tick_ms=45 if spike else 12,target_tick_ms=16.667,online_players=online,incident=spike))
            tick+=1
            await s.wait(1)
    await asyncio.gather(coordinator(),gameplay(),sessions(),servers())
    for source in sources.values():
        source.flush_delayed(True)
        if source.client.flush(30) or source.errors:
            raise RuntimeError('Unacknowledged source events')
    ACTIVE.set(0)
    print(json.dumps({'run_id':run_id,'status':'all deliveries acknowledged'}),flush=True)

if __name__=='__main__': asyncio.run(run())
