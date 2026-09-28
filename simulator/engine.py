"""Small deterministic domain model; delivery faults never reapply an action."""
import random
from dataclasses import dataclass, field

@dataclass
class Match:
    seed: int
    count: int = 100
    alive: list = field(init=False)
    health: dict = field(init=False)
    outcomes: dict = field(init=False)
    def __post_init__(self):
        if not 2 <= self.count <= 100:
            raise ValueError('players must be between 2 and 100')
        self.rng = random.Random(self.seed)
        self.alive = [f'player-{i:03d}' for i in range(1, self.count+1)]
        self.health = dict.fromkeys(self.alive, 100)
        self.outcomes = {p: dict(player_id=p, kills=0, damage=0, placement=0) for p in self.alive}
        self.attacker = self.victim = None
    def step(self):
        if len(self.alive) == 1:
            return []
        if self.victim is None:
            self.attacker, self.victim = self.rng.sample(self.alive, 2)
        a, v = self.attacker, self.victim
        before = self.health[v]
        damage = min(before, 50)
        self.health[v] -= damage
        self.outcomes[a]['damage'] += damage
        events = [('damage_dealt', a, dict(victim_id=v, damage=damage, health_before=before, health_after=self.health[v]))]
        if not self.health[v]:
            placement = len(self.alive)
            self.outcomes[a]['kills'] += 1
            self.outcomes[v]['placement'] = placement
            self.alive.remove(v)
            events.append(('player_eliminated', a, dict(victim_id=v, placement=placement, alive=len(self.alive))))
            self.victim = None
            if len(self.alive) == 1:
                self.outcomes[self.alive[0]]['placement'] = 1
        return events
    def result(self):
        if len(self.alive) != 1:
            raise ValueError('match still running')
        return sorted(self.outcomes.values(), key=lambda x: x['placement'])
