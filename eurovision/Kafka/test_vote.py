from kafka import KafkaProducer
import json
import uuid
 
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
 
vote = {
    "vote_id": str(uuid.uuid4()),
    "country": "Belgium",
    "voter_ip": "192.168.1.1"
}
 
producer.send("votes", value=vote)
producer.flush()
print(f"Stem verstuurd: {vote}")
