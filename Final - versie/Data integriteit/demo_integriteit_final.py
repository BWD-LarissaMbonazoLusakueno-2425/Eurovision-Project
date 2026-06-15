import uuid, random
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args

LANDEN = ["Belgium","Netherlands","France","Germany","Italy",
          "Spain","Sweden","Norway","Ukraine","Portugal"]
N = 1_000_000

script_start = datetime.now()

print("="*60)
print(" Eurovision Audit Systeem - Data Integriteit Test")
print(" NFR-03: Geen dataverlies")
print(" Odisee Hogeschool - Mega Data Challenge 2025-2026")
print(f" Gestart op: {script_start.strftime('%Y-%m-%d %H:%M:%S')}")
print("="*60)
print()

cluster = Cluster(["localhost"])
session = cluster.connect("eurovision")

# STAP 0 - TABEL LEEGMAKEN
print("STAP 0 - Tabel leegmaken voor schone test...")
session.execute("TRUNCATE votes")
print("  Tabel geleegd. Start met 0 stemmen.")
print()

# STAP 1 - VOOR
voor = 0
for land in LANDEN:
    count = session.execute(
        "SELECT COUNT(*) FROM votes WHERE country=%s", [land]
    ).one()[0]
    voor += count
print(f"STAP 1 - Baseline telling:")
print(f"  Stemmen in Cassandra VOOR test : {voor:>10,}")
print()

# STAP 2 - INVOEGEN (CONCURRENT + PREPARED STATEMENT)
print(f"STAP 2 - Exact {N:,} stemmen invoegen (concurrent)...")
insert_start = datetime.now()

prepared = session.prepare(
    "INSERT INTO votes (country, timestamp, vote_id, voter_ip) VALUES (?, ?, ?, ?)"
)

CONCURRENCY = 200
CHUNK       = 50_000

ingevoegd = 0
for chunk_start in range(0, N, CHUNK):
    chunk_size = min(CHUNK, N - chunk_start)

    params = [
        (
            random.choice(LANDEN),
            datetime.now(),
            uuid.uuid4(),
            f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"
        )
        for _ in range(chunk_size)
    ]

    results = execute_concurrent_with_args(
        session, prepared, params, concurrency=CONCURRENCY, raise_on_first_error=False
    )

    fouten = [r for (success, r) in results if not success]
    if fouten:
        print(f"  WAARSCHUWING: {len(fouten)} inserts gefaald in deze chunk")
    ingevoegd += chunk_size

    elapsed = (datetime.now() - insert_start).total_seconds()
    snelheid = ingevoegd / elapsed if elapsed > 0 else 0
    print(f"  {ingevoegd:>9,} ingevoegd | {elapsed:6.1f}s verstreken | {snelheid:8,.0f} inserts/sec")

insert_duur = (datetime.now() - insert_start).seconds
print(f"  Invoegen klaar in {insert_duur} seconden")
print()

# STAP 3 - NA (per land)
print(f"STAP 3 - Telling per land na invoegen:")
print(f"  {'Land':<15} {'Stemmen':>10}")
print(f"  {'-'*15} {'-'*10}")
na = 0
for land in LANDEN:
    count = session.execute(
        "SELECT COUNT(*) FROM votes WHERE country=%s", [land]
    ).one()[0]
    na += count
    print(f"  {land:<15} {count:>10,}")
print(f"  {'-'*15} {'-'*10}")
print(f"  {'TOTAAL':<15} {na:>10,}")
print()

# STAP 4 - VERIFICATIE
verschil = na - voor
script_duur = (datetime.now() - script_start).seconds
minuten = script_duur // 60
seconden = script_duur % 60

print(f"STAP 4 - Verificatie NFR-03:")
print(f"  Stemmen VOOR   : {voor:>10,}")
print(f"  Stemmen NA     : {na:>10,}")
print(f"  Verschil       : {verschil:>10,}")
print(f"  Verwacht       : {N:>10,}")
print()
print("="*60)
if verschil == N:
    print(" GESLAAGD - NFR-03 VOLDAAN")
    print(f" {N:,} stemmen ingevoegd, {verschil:,} gevonden")
    print(" Geen dataverlies in Cassandra")
else:
    verlies = abs(N - verschil)
    print(" GEFAALD - NFR-03 NIET VOLDAAN")
    print(f" {verlies:,} stemmen verloren")
print("="*60)
print(f" Gestart  : {script_start.strftime('%Y-%m-%d %H:%M:%S')}")
print(f" Gestopt  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f" Invoegen : {insert_duur} seconden")
print(f" Totale uitvoeringstijd: {minuten} min {seconden} sec")
print("="*60)

cluster.shutdown()

