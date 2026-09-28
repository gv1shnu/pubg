import json
from pathlib import Path
import pytest
from jsonschema import Draft202012Validator,FormatChecker
from pydantic import ValidationError
from simulator.contracts import validate
from tests.fixtures import fixture

SCHEMA=json.loads(Path('contracts/events.v1.schema.json').read_text())

def test_contract_and_schema_agree_for_complete_match():
    Draft202012Validator.check_schema(SCHEMA)
    schema=Draft202012Validator(SCHEMA,format_checker=FormatChecker())
    for _,event in fixture():
        schema.validate(event);assert validate(event)==event

@pytest.mark.parametrize('field,value',[('schema_version',999),('simulated',False),('sequence',0),('event_time','no date'),('unexpected',1)])
def test_bad_envelope_rejected(field,value):
    event=dict(fixture()[0][1]);event[field]=value
    with pytest.raises(ValidationError):validate(event)

def test_damage_relational_validation():
    event=next(e for _,e in fixture() if e['event_type']=='damage_dealt')
    event['payload']['health_after']=99
    with pytest.raises(ValidationError):validate(event)

def test_identity_repeatability():
    first=fixture();second=fixture()
    assert [(t,e['event_id'],e['payload']) for t,e in first]==[(t,e['event_id'],e['payload']) for t,e in second]
    assert fixture('another')[0][1]['event_id']!=first[0][1]['event_id']
