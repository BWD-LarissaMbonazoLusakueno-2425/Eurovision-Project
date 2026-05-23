from kafka import KafkaProducer
from concurrent.futures import ThreadPoolExecutor
import json, random, time, threading

COUNTRIES = ["Belgium","Netherlands","France","Germany","Italy",
             "Spain","Sweden","Norway","Ukraine","Portugal"]

BOOTSTRAP = "35.205.160.140:9092"
TOPIC     = "eurovision-votes"
NUM_WORKERS  = 8
TOTAL_VOTES  = 100_000
BATCH_SIZE   = 500
MEASURE_WINDOW = 5  # seconden per throughput-meting

# Gedeelde teller (thread-safe)
lock        = threading.Lock()
sent        = [0]
start_time  = [None]
window_sent = [0]
window_start= [None]

def make_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        batch_size=65536,       # grotere Kafka batch buffer
        linger_ms=5,            # wacht 5ms om berichten te groeperen
        compression_type="lz4", # snelle compressie, minder netwerk-overhead
        acks=1,                 # alleen leader bevestigt (sneller dan acks=all)
    )

def worker(votes_to_send):
    producer = make_producer()  # elke thread eigen producer
    for _ in range(votes_to_send // BATCH_SIZE):
        for _ in range(BATCH_SIZE):
            vote = {"country": random.choice(COUNTRIES), "timestamp": time.time()}
            producer.send(TOPIC, vote)
        producer.flush()

        with lock:
            sent[0] += BATCH_SIZE
            window_sent[0] += BATCH_SIZE
            now = time.time()
            elapsed_total = now - start_time[0]
            elapsed_window = now - window_start[0]

            if elapsed_window >= MEASURE_WINDOW:
                tps = window_sent[0] / elapsed_window
                total_tps = sent[0] / elapsed_total
                pct = total_tps / 200_000 * 100
                print(f"[t={elapsed_total:5.1f}s] "
                      f"venster: {tps:8,.0f} ev/sec | "
                      f"gemiddeld: {total_tps:8,.0f} ev/sec | "
                      f"{sent[0]:,}/{TOTAL_VOTES:,} stemmen | "
                      f"{pct:.1f}% van 200K doel")
                window_sent[0] = 0
                window_start[0] = now

    producer.flush()

if __name__ == "__main__":
    print(f"Start load test: {TOTAL_VOTES:,} stemmen, {NUM_WORKERS} workers, batch={BATCH_SIZE}")
    print(f"Kafka broker: {BOOTSTRAP}\n")

    start_time[0]  = time.time()
    window_start[0]= time.time()

    per_worker = TOTAL_VOTES // NUM_WORKERS
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as ex:
        futures = [ex.submit(worker, per_worker) for _ in range(NUM_WORKERS)]

    elapsed = time.time() - start_time[0]
    avg_tps = TOTAL_VOTES / elapsed
    pct = avg_tps / 200_000 * 100

    print(f"\n{'='*55}")
    print(f"Klaar! {TOTAL_VOTES:,} stemmen in {elapsed:.1f}s")
    print(f"Gemiddelde throughput : {avg_tps:,.0f} events/sec")
    print(f"% van 200K doel       : {pct:.1f}%")
    if avg_tps < 10_000:
        print("Bottleneck: netwerk (tunneling/internet latency)")
    elif avg_tps < 50_000:
        print("Bottleneck: Python threads of Kafka broker config")
    else:
        print("Goed resultaat voor een PoC op 1 VM!")
    print('='*55)
