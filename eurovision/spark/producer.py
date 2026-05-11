import json
import random
import time
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='dc1-vm1:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

countries = ["BE", "FR", "NL", "DE", "ES", "IT", "SE", "NO", "FI", "DK"]

count = 0
start = time.time()

while True:
    vote = {
        "vote_id": str(count),
        "from_country": random.choice(countries),
        "to_country": random.choice(countries),
        "phone_number": f"+3{random.randint(100000000, 999999999)}",
        "vote_time": datetime.utcnow().isoformat()
    }

    producer.send("votes", vote)

    count += 1

    # snelheid tonen
    if count % 10000 == 0:
        elapsed = time.time() - start
        rate = int(count / elapsed)

        print(f"⚡ Sent: {count} | {rate} events/sec")