from kafka import KafkaProducer
import json
import random
import time
COUNTRIES = ["Belgium","Netherlands","France","Germany","Italy","Spain","Sweden","Norway","Ukraine","Portugal"]
producer = KafkaProducer(bootstrap_servers="35.205.160.140:9092", value_serializer=lambda v: json.dumps(v).encode("utf-8"))
print("Starting vote simulation...")
for i in range(1000):
    vote = {"country": random.choice(COUNTRIES), "timestamp": time.time()}
    producer.send("eurovision-votes", vote)
    print(f"Vote {i+1}: {vote[chr(99)+chr(111)+chr(117)+chr(110)+chr(116)+chr(114)+chr(121)]}")
    time.sleep(0.1)
producer.flush()
print("Done! 1000 votes sent.")