"""Version 1 wire contracts. All game and server measurements are synthetic."""
from datetime import datetime
from typing import Annotated, Literal, Union
from pydantic import BaseModel, ConfigDict, Field, AwareDatetime, model_validator

class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")

class Outcome(Strict):
    player_id: str
    kills: int = Field(ge=0)
    damage: int = Field(ge=0)
    placement: int = Field(ge=1, le=100)

class Created(Strict):
    players: list[str] = Field(min_length=2, max_length=100)
    region: str
    map: str
    duration_seconds: int = Field(ge=10, le=600)

class Started(Strict):
    alive: int = Field(ge=2, le=100)

class Zone(Strict):
    phase: int = Field(ge=1, le=10)
    radius_m: float = Field(gt=0)

class Finished(Strict):
    outcomes: list[Outcome] = Field(min_length=2, max_length=100)
    expected_gameplay_count: int = Field(ge=1)
    gameplay_sequence_max: int = Field(ge=1)
    expected_event_ids: list[str] = Field(min_length=1)

    @model_validator(mode="after")
    def placements(self):
        assert sorted(x.placement for x in self.outcomes) == list(range(1, len(self.outcomes)+1)), "placements must be unique and complete"
        assert len({x.player_id for x in self.outcomes}) == len(self.outcomes), "duplicate player"
        assert sum(x.kills for x in self.outcomes) == len(self.outcomes)-1, "kills do not match eliminations"
        assert len(set(self.expected_event_ids)) == self.expected_gameplay_count == self.gameplay_sequence_max, "invalid manifest bounds"
        return self

class Damage(Strict):
    victim_id: str
    damage: int = Field(ge=1, le=100)
    health_before: int = Field(ge=1, le=100)
    health_after: int = Field(ge=0, le=99)
    @model_validator(mode="after")
    def health(self):
        assert self.health_before-self.damage == self.health_after, "inconsistent damage"
        return self

class Elimination(Strict):
    victim_id: str
    placement: int = Field(ge=2, le=100)
    alive: int = Field(ge=1, le=99)

class Position(Strict):
    x: float = Field(ge=0, le=1000)
    y: float = Field(ge=0, le=1000)
    alive: int = Field(ge=1, le=100)

class Session(Strict):
    session_id: str
    connected: bool

class Server(Strict):
    cpu_pct: float = Field(ge=0, le=100)
    memory_pct: float = Field(ge=0, le=100)
    latency_ms: list[float] = Field(min_length=1, max_length=100)
    packets_sent: int = Field(ge=1)
    packets_lost: int = Field(ge=0)
    tick_ms: float = Field(gt=0)
    target_tick_ms: float = Field(gt=0)
    online_players: int = Field(ge=0, le=300)
    incident: bool
    @model_validator(mode="after")
    def packets(self):
        assert self.packets_lost <= self.packets_sent and min(self.latency_ms) >= 0
        return self

class Envelope(Strict):
    event_id: str = Field(min_length=1)
    schema_version: Literal[1]
    run_id: str = Field(min_length=1)
    producer_id: str = Field(min_length=1)
    event_time: AwareDatetime
    emitted_at: AwareDatetime
    sequence: int = Field(ge=1)
    match_id: str = Field(min_length=1)
    server_id: str = Field(min_length=1)
    player_id: str | None = None
    simulated: Literal[True]
    simulation_elapsed_seconds: float = Field(ge=0)
    @model_validator(mode="after")
    def session_consistency(self):
        if self.event_type in ('player_connected','player_disconnected','player_reconnected'):
            assert self.payload.connected == (self.event_type != 'player_disconnected'), 'invalid session transition payload'
        return self

class MatchCreated(Envelope):
    event_type: Literal['match_created']
    payload: Created
class MatchStarted(Envelope):
    event_type: Literal['match_started']
    payload: Started
class ZoneChanged(Envelope):
    event_type: Literal['zone_changed']
    payload: Zone
class MatchFinished(Envelope):
    event_type: Literal['match_finished']
    payload: Finished
class DamageDealt(Envelope):
    player_id: str
    event_type: Literal['damage_dealt']
    payload: Damage
class PlayerEliminated(Envelope):
    player_id: str
    event_type: Literal['player_eliminated']
    payload: Elimination
class PositionSample(Envelope):
    player_id: str
    event_type: Literal['player_position_sample']
    payload: Position
class Connected(Envelope):
    player_id: str
    event_type: Literal['player_connected']
    payload: Session
class Disconnected(Envelope):
    player_id: str
    event_type: Literal['player_disconnected']
    payload: Session
class Reconnected(Envelope):
    player_id: str
    event_type: Literal['player_reconnected']
    payload: Session
class ServerSample(Envelope):
    event_type: Literal['server_sample']
    payload: Server

Event = Annotated[Union[MatchCreated, MatchStarted, ZoneChanged, MatchFinished, DamageDealt,
    PlayerEliminated, PositionSample, Connected, Disconnected, Reconnected, ServerSample], Field(discriminator='event_type')]

from pydantic import TypeAdapter
EVENT = TypeAdapter(Event)
TOPICS = {"match": "match.v1", "gameplay": "gameplay.v1", "session": "session.v1", "server": "server.v1"}

def validate(event):
    return EVENT.validate_python(event).model_dump(mode='json')

if __name__ == '__main__':
    import json
    from pathlib import Path
    schema = EVENT.json_schema()
    schema['$schema'] = 'https://json-schema.org/draft/2020-12/schema'
    Path('contracts/events.v1.schema.json').write_text(json.dumps(schema, indent=2)+'\n')
