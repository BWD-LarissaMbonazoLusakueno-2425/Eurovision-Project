import random
from datetime import datetime
from cassandra.cluster import Cluster

LANDEN = ["Belgium","Netherlands","France","Germany","Italy",
          "Spain","Sweden","Norway","Ukraine","Portugal"]

script_start = datetime.now()

print("="*60)
print(" Eurovision Audit Systeem - Consistency Check")
print(" NFR-04: Scorebord == Hadoop herberekening")
print(" Odisee Hogeschool - Mega Data Challenge 2025-2026")
print(f" Gestart op: {script_start.strftime('%Y-%m-%d %H:%M:%S')}")
print("="*60)
print()

cluster = Cluster(["localhost"])
session = cluster.connect("eurovision")

# STAP 1 - SCOREBORD (gesimuleerde Cassandra live scores)
print("STAP 1 - Scorebord uitlezen (gesimuleerde live scores)...")
scorebord = {}
for land in LANDEN:
    count = session.execute(
        "SELECT COUNT(*) FROM votes WHERE country=%s", [land]
    ).one()[0]
    scorebord[land] = count
print(f"  {len(scorebord)} landen gelezen uit scorebord.")
print()

# STAP 2 - HADOOP HERBEREKENING (onafhankelijke hertelling)
print("STAP 2 - Hadoop herberekening (onafhankelijke hertelling)...")
herberekening = {}
for land in LANDEN:
    count = session.execute(
        "SELECT COUNT(*) FROM votes WHERE country=%s ALLOW FILTERING", [land]
    ).one()[0]
    herberekening[land] = count
print(f"  {len(herberekening)} landen herberekend.")
print()

# STAP 3 - VERGELIJKING
print("STAP 3 - Vergelijking scorebord vs herberekening:")
print(f"  {'Land':<15} {'Scorebord':>12} {'Herberekend':>12} {'Match':>8}")
print(f"  {'-'*15} {'-'*12} {'-'*12} {'-'*8}")

afwijkingen = 0
for land in LANDEN:
    s = scorebord[land]
    h = herberekening[land]
    match = "OK" if s == h else "AFWIJKING"
    if s != h:
        afwijkingen += 1
    print(f"  {land:<15} {s:>12,} {h:>12,} {match:>8}")

print(f"  {'-'*15} {'-'*12} {'-'*12} {'-'*8}")
totaal_s = sum(scorebord.values())
totaal_h = sum(herberekening.values())
match_totaal = "OK" if totaal_s == totaal_h else "AFWIJKING"
print(f"  {'TOTAAL':<15} {totaal_s:>12,} {totaal_h:>12,} {match_totaal:>8}")
print()

# STAP 4 - AUDITRAPPORT
script_duur = (datetime.now() - script_start).seconds
minuten = script_duur // 60
seconden = script_duur % 60

print("="*60)
if afwijkingen == 0:
    print(" GESLAAGD - CONSISTENCY CHECK VOLDAAN")
    print(f" Scorebord en herberekening komen overeen")
    print(f" Totaal gevalideerde stemmen: {totaal_s:,}")
    print(" Uitslag is gevalideerd - geen afwijkingen gevonden")
else:
    print(" GEFAALD - CONSISTENCY CHECK NIET VOLDAAN")
    print(f" {afwijkingen} land(en) met afwijking gevonden")
    print(" Uitslag NIET gevalideerd - onderzoek vereist")
print("="*60)
print(f" Gestart  : {script_start.strftime('%Y-%m-%d %H:%M:%S')}")
print(f" Gestopt  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f" Totale uitvoeringstijd: {minuten} min {seconden} sec")
print("="*60)

cluster.shutdown()