import uuid
from datetime import datetime,timezone,timedelta
from simulator.engine import Match
from simulator.contracts import validate

def fixture(run_id='fixture',seed=42,count=4):
    model=Match(seed,count);events=[];sequence={};base=datetime.now(timezone.utc)-timedelta(seconds=2)
    def event(source,kind,payload,player=None):
        sequence[source]=sequence.get(source,0)+1
        n=sequence[source]
        e=validate(dict(event_id=str(uuid.uuid5(uuid.NAMESPACE_URL,f'{run_id}/{source}/{n}')),event_type=kind,schema_version=1,run_id=run_id,producer_id=source,
            event_time=base.isoformat(),emitted_at=datetime.now(timezone.utc).isoformat(),sequence=n,match_id='match-1',server_id='server-1',player_id=player,simulated=True,simulation_elapsed_seconds=float(n),payload=payload))
        events.append((source+'.v1',e));return e
    event('match','match_created',dict(players=list(model.alive),region='ap-south',map='Amber Basin',duration_seconds=10))
    event('match','match_started',dict(alive=count))
    event('session','player_connected',dict(session_id='s1',connected=True),'player-001')
    event('server','server_sample',dict(cpu_pct=30,memory_pct=40,latency_ms=[10,20,30,40],packets_sent=100,packets_lost=2,tick_ms=12,target_tick_ms=16.667,online_players=1,incident=False))
    while len(model.alive)>1:
        for kind,player,payload in model.step():event('gameplay',kind,payload,player)
    ids=[e['event_id'] for topic,e in events if topic=='gameplay.v1']
    event('match','match_finished',dict(outcomes=model.result(),expected_gameplay_count=len(ids),gameplay_sequence_max=len(ids),expected_event_ids=ids))
    event('session','player_disconnected',dict(session_id='s1',connected=False),'player-001')
    return events
