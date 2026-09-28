import pytest
from simulator.engine import Match

@pytest.mark.parametrize('seed',range(20))
@pytest.mark.parametrize('players',[2,4,100])
def test_match_invariants(seed,players):
    match=Match(seed,players);dead=set();damage=0;eliminations=0
    while len(match.alive)>1:
        for kind,attacker,payload in match.step():
            assert attacker not in dead
            victim=payload['victim_id']; assert victim not in dead
            if kind=='damage_dealt':
                assert payload['health_before']-payload['damage']==payload['health_after']
                assert match.health[victim]>=0;damage+=payload['damage']
            else:
                assert match.health[victim]==0;dead.add(victim);eliminations+=1
    result=match.result()
    assert sorted(p['placement'] for p in result)==list(range(1,players+1))
    assert sum(p['kills'] for p in result)==eliminations==players-1
    assert sum(p['damage'] for p in result)==damage==100*(players-1)
    other=Match(seed,players)
    while len(other.alive)>1:other.step()
    assert result==other.result()

def test_incomplete_result_rejected():
    with pytest.raises(ValueError):Match(42,4).result()
