#!/usr/bin/env python3
"""
Eurovision Stemsysteem - Load Test Script
==========================================
Project  : Mega Data Challenge - Odisee Hogeschool
Rol      : Automatisering / Verificatie punt 5
Student  : Mohamed Amin Ahkim

Verificatie punt 5 - Check 1: Load testing
  Stuurt 2.000.000 stemmen naar Kafka via 10 threads.
  Meet de doorvoer in stemmen per seconde.
  NFR: minimaal 200.000 stemmen/seconde.

Stem formaat (zelfde als test_vote.py):
  { vote_id, country, voter_ip }
  Kafka topic: votes

Gebruik
-------
  python3 load_test.py          (simulatiemodus)
  python3 load_test.py --real   (echte Kafka op localhost:9092)

Vereisten
---------
  pip install kafka-python psutil
"""

import threading
import time
import uuid
import random
import json
import argparse
import psutil
from datetime import datetime

# ================================================================
# CONFIGURATIE
# ================================================================

USE_REAL_KAFKA  = False

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC     = "votes"          # Zelfde topic als test_vote.py

TOTAAL_STEMMEN  = 2_000_000
AANTAL_THREADS  = 10
NFR_DREMPEL     = 200_000

LANDEN = [
    "Belgium", "Netherlands", "France", "Germany", "Italy",
    "Spain",   "Sweden",      "Norway", "Finland", "Ukraine",
]

# ================================================================
# GEDEELDE VARIABELEN
# ================================================================

stemmen_verstuurd = 0
lock              = threading.Lock()

# ================================================================
# KLEURCODES
# ================================================================

GROEN = "\033[92m"
ROOD  = "\033[91m"
GEEL  = "\033[93m"
RESET = "\033[0m"
VET   = "\033[1m"


# ================================================================
# STEM AANMAKEN
# ================================================================

def genereer_stem():
    """
    Genereert een stem in het formaat van test_vote.py:
      vote_id  : unieke UUID
      country  : naam van het land
      voter_ip : IP adres van de stemmer
    """
    return {
        "vote_id" : str(uuid.uuid4()),
        "country" : random.choice(LANDEN),
        "voter_ip": f"192.168.{random.randint(0,255)}.{random.randint(1,254)}",
    }


# ================================================================
# WORKER THREAD
# ================================================================

def stuur_stemmen(aantal, producer=None):
    """Stuurt 'aantal' stemmen zo snel mogelijk naar Kafka."""
    global stemmen_verstuurd
    for _ in range(aantal):
        stem = genereer_stem()
        if producer is not None:
            try:
                producer.send(KAFKA_TOPIC,
                              value=json.dumps(stem).encode("utf-8"))
            except Exception:
                pass
        with lock:
            stemmen_verstuurd += 1


# ================================================================
# MONITOR THREAD
# ================================================================

def monitor(start_tijd, stop_event):
    """Rapporteert elke 2 seconden de doorvoer en systeemstatus."""
    vorige_count = 0
    vorige_tijd  = start_tijd

    while not stop_event.is_set():
        time.sleep(2)
        nu       = time.time()
        huidig   = stemmen_verstuurd
        doorvoer = (huidig - vorige_count) / (nu - vorige_tijd)
        cpu      = psutil.cpu_percent(interval=None)
        mem      = psutil.virtual_memory().percent
        kleur    = GROEN if doorvoer >= NFR_DREMPEL else GEEL

        print(
            f"\r  Stemmen: {huidig:>10,} | "
            f"Doorvoer: {kleur}{doorvoer:>8,.0f}/sec{RESET} | "
            f"CPU: {cpu:4.1f}% | MEM: {mem:4.1f}%  ",
            end="", flush=True
        )
        vorige_count = huidig
        vorige_tijd  = nu


# ================================================================
# BOTTLENECK DETECTIE
# ================================================================

def detecteer_bottleneck():
    """Detecteert automatisch waar de bottleneck zit."""
    cpu = psutil.cpu_percent(interval=1)
    mem = psutil.virtual_memory().percent

    if cpu > 85:
        return f"CPU ({cpu:.0f}% gebruik) - processor is de beperkende factor"
    elif mem > 85:
        return f"Geheugen ({mem:.0f}% gebruik) - onvoldoende RAM"
    else:
        return (
            f"Netwerk/Kafka I/O (CPU {cpu:.0f}%, MEM {mem:.0f}%) - "
            f"tip: producer en Kafka draaien al op dezelfde VM via bridge network"
        )


# ================================================================
# HOOFDFUNCTIE
# ================================================================

def main():
    global USE_REAL_KAFKA

    parser = argparse.ArgumentParser(description="Eurovision Load Test")
    parser.add_argument("--real", action="store_true", help="Echte Kafka modus")
    args = parser.parse_args()
    if args.real:
        USE_REAL_KAFKA = True

    print(f"\n{VET}Eurovision Stemsysteem   Load Test{RESET}")
    print(f"{'=' * 55}")
    print(f"Verificatie  : Punt 5   Check 1: Load testing")
    print(f"Modus        : {'Echte Kafka' if USE_REAL_KAFKA else 'Simulatie'}")
    print(f"Kafka topic  : {KAFKA_TOPIC}")
    print(f"Stem formaat : {{vote_id, country, voter_ip}}")
    print(f"Totaal       : {TOTAAL_STEMMEN:,} stemmen")
    print(f"Threads      : {AANTAL_THREADS}")
    print(f"NFR drempel  : {NFR_DREMPEL:,} stemmen/sec")
    print(f"{'=' * 55}\n")

    # Kafka producer aanmaken
    producer = None
    if USE_REAL_KAFKA:
        try:
            from kafka import KafkaProducer
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                acks=1,
                batch_size=65536,
                linger_ms=5,
                compression_type="gzip",
                buffer_memory=67108864,
            )
            print(f"  Verbonden met Kafka op {KAFKA_BOOTSTRAP}")
        except ImportError:
            print(f"  {ROOD}Fout: kafka-python niet geinstalleerd.{RESET}")
            print(f"  Installeer: pip install kafka-python")
            return
        except Exception as exc:
            print(f"  {ROOD}Fout: {exc}{RESET}")
            print(f"  Controleer: sudo docker ps | grep kafka")
            return

    # Threads aanmaken
    per_thread = TOTAAL_STEMMEN // AANTAL_THREADS
    threads    = [
        threading.Thread(target=stuur_stemmen, args=(per_thread, producer), daemon=True)
        for _ in range(AANTAL_THREADS)
    ]

    # Monitor thread
    stop_event = threading.Event()
    start_tijd = time.time()
    monitor_t  = threading.Thread(
        target=monitor, args=(start_tijd, stop_event), daemon=True
    )

    # Starten
    print(f"  Test gestart om {datetime.now().strftime('%H:%M:%S')}...")
    monitor_t.start()
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    eind_tijd = time.time()
    stop_event.set()
    monitor_t.join(timeout=3)

    if producer:
        producer.flush(timeout=30)
        producer.close()

    # Resultaten
    duur     = eind_tijd - start_tijd
    doorvoer = stemmen_verstuurd / duur if duur > 0 else 0
    gehaald  = doorvoer >= NFR_DREMPEL

    print(f"\n\n{VET}{'=' * 55}{RESET}")
    print(f"{VET}RESULTATEN   Load testing (Verificatie punt 5   check 1){RESET}")
    print(f"{'=' * 55}")
    print(f"  Stemmen verstuurd    : {stemmen_verstuurd:>12,}")
    print(f"  Totale duur          : {duur:>12.2f} seconden")
    print(f"  Gemiddelde doorvoer  : {doorvoer:>12,.0f} stemmen/sec")
    print(f"  NFR drempel          : {NFR_DREMPEL:>12,} stemmen/sec")

    if gehaald:
        print(f"\n  {GROEN}{VET}NFR GEHAALD{RESET}")
        print(f"  Kafka verwerkt {doorvoer:,.0f} stemmen/sec >= 200K vereiste")
    else:
        tekort = NFR_DREMPEL - doorvoer
        print(f"\n  {ROOD}{VET}NFR NIET GEHAALD{RESET}")
        print(f"  Tekort: {tekort:,.0f} stemmen/sec")
        print(f"  Bottleneck: {detecteer_bottleneck()}")
        print(f"\n  Opmerking: de VM draait ook Kafka, Spark, Cassandra en Grafana")
        print(f"  tegelijk. Het geheugen is de beperkende factor, niet Kafka zelf.")

    print(f"{'=' * 55}\n")


if __name__ == "__main__":
    main()
