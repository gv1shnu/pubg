import time
from confluent_kafka import Consumer,TopicPartition
from simulator.contracts import TOPICS
client=Consumer({'bootstrap.servers':'kafka:9092','group.id':'battleground-v1','enable.auto.commit':False})
try:
    end=time.monotonic()+120
    while time.monotonic()<end:
        metadata=client.list_topics(timeout=5)
        parts=[TopicPartition(t,p) for t in TOPICS.values() for p in metadata.topics[t].partitions]
        lag=0
        for p in client.committed(parts,timeout=5):
            low,high=client.get_watermark_offsets(p,timeout=5)
            lag+=max(0,high-max(low,p.offset))
        if lag==0:
            print('Checkpoint-committed lag drained');break
        time.sleep(2)
    else: raise TimeoutError('Lag did not drain within 120 seconds')
finally: client.close()
