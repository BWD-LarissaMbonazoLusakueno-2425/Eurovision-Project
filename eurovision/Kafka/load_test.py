import threading
import time
import uuid
import random
from kafka import KafkaProducer
import json
 
USE_REAL_KAFKA = False
TOTAL_VOTES = 200000
NUM_THREADS = 10
COUNTRIES = ["Belgium", "Netherlands", "France", "Germany",
             "Italy", "Spain", "Sweden", "Norway",
             "Finland", "Ukraine"]
 
votes_sent = 0
lock = threading.Lock()
 
def simulate_votes(thread_id, num_votes):
    global votes_sent
    for _ in range(num_votes):
        vote = {
            "vote_id": str(uuid.uuid4()),
            "country": random.choice(COUNTRIES),
            "voter_ip": f"192.168.{random.randint(0,255)}.{random.randint(0,255)}"
        }
        if USE_REAL_KAFKA:
            producer.send("votes", value=vote)
        with lock:
            votes_sent += 1
 
if USE_REAL_KAFKA:
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
 
print(f"Loadtest gestart met {NUM_THREADS} threads...")
start = time.time()
threads = []
votes_per_thread = TOTAL_VOTES // NUM_THREADS
for i in range(NUM_THREADS):
    t = threading.Thread(target=simulate_votes, args=(i, votes_per_thread))
    threads.append(t)
    t.start()
for t in threads:
    t.join()
 
end = time.time()
duur = end - start
print(f"Totaal stemmen verstuurd: {votes_sent}")
print(f"Tijd: {duur:.2f} seconden")
print(f"Throughput: {votes_sent / duur:.0f} stemmen per seconde")
