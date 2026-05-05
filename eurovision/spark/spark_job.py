from kafka import KafkaConsumer
from cassandra.cluster import Cluster
import json

consumer = KafkaConsumer(
    "eurovision-votes",
    bootstrap_servers="35.205.160.140:9092",
    auto_offset_reset="earliest",
    consumer_timeout_ms=10000
)
votes = {}
for msg in consumer:
    data = json.loads(msg.value)
    country = data["country"]
    votes[country] = votes.get(country, 0) + 1
cluster = Cluster(["34.22.143.37"])
session = cluster.connect("eurovision")
print("Results:")
for country, cnt in sorted(votes.items(), key=lambda x: -x[1]):
    print(f"{country}: {cnt} votes")
    session.execute("INSERT INTO votes (country, votes) VALUES (%s, %s)", (country, cnt))