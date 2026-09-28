# Event contract v1

`events.v1.schema.json` is the versioned wire shape. `simulator/contracts.py` defines the producer's typed models and can regenerate the schema in the cloud build. Both validators enforce field types; Python/Java add relational checks for damage, manifests and packet counts. Flink also checks source-topic membership, namespace syntax and timestamps. Wire timestamps require explicit timezone offsets; emitted events use UTC.

Topics: `match.v1`, `gameplay.v1`, `session.v1`, `server.v1`, `dead-letter.v1`, `excessively-late.v1`. Each has two partitions, replication factor one and six-hour retention. Match/gameplay keys are namespace/run/match; session/server keys are namespace/run/server. Ordering applies within a topic partition, never across topics. Replay namespace is a Kafka header, not a rewritten source run/event ID.

Domain fixtures in `tests/fixtures.py` provide valid complete examples. Invalid records preserve their original body plus topic/partition/offset and reason in the DLQ and rejection table. Limit envelope size to 256 KiB. Repeated source IDs with conflicting payloads are outside the trusted simulator contract: first accepted canonical row wins; do not use this demo as an adversarial ingestion endpoint.
