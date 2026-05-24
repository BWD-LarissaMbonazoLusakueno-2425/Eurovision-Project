#!/usr/bin/env python3
"""
Eurovision Stemsysteem - Controle en Audit Script
==================================================
Project  : Mega Data Challenge - Odisee Hogeschool
Rol      : Automatisering / Verificatie punt 5
Student  : Mohamed Amin Ahkim

Cassandra schema:
  KEYSPACE : eurovision
  TABLE    : votes
  KOLOMMEN : vote_id (uuid), country (text),
             timestamp (timestamp), voter_ip (text)

Verificatie punt 5:
  Check 2 - Data integriteit:
      1. Tel stemmen VOOR het invoegen (baseline)
      2. Voeg exact N stemmen in (standaard: 1000)
      3. Tel stemmen NA het invoegen
      4. Controleer: NA - VOOR = N
      NFR-03: geen dataverlies.

  Check 3 - Interne consistency check:
      Vergelijkt COUNT(*) globaal met som van landen apart.
      Als die twee gelijk zijn: data intern consistent.

Gebruik
-------
  python3 controle_script.py              (simulatie)
  python3 controle_script.py --real       (echte Cassandra)
  python3 controle_script.py --real --n 500  (500 teststemmen)
"""

import json
import uuid
import random
import argparse
from datetime import datetime

# ================================================================
# CONFIGURATIE
# ================================================================

USE_REAL_CASSANDRA = False

CASSANDRA_HOST     = "localhost"
CASSANDRA_PORT     = 9042
CASSANDRA_KEYSPACE = "eurovision"
CASSANDRA_TABLE    = "votes"

LANDEN = [
    "Belgium", "Netherlands", "France", "Germany", "Italy",
    "Spain",   "Sweden",      "Norway", "Finland", "Ukraine",
]

# ================================================================
# KLEURCODES
# ================================================================

GROEN = "\033[92m"
ROOD  = "\033[91m"
RESET = "\033[0m"
VET   = "\033[1m"
DIM   = "\033[2m"


# ================================================================
# CASSANDRA HULPFUNCTIES
# ================================================================

def cassandra_verbinding():
    """Maakt verbinding met Cassandra."""
    try:
        from cassandra.cluster import Cluster
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect(CASSANDRA_KEYSPACE)
        return session, cluster
    except ImportError:
        print(f"  {ROOD}Fout: cassandra-driver niet geinstalleerd.{RESET}")
        print(f"  Installeer: pip install cassandra-driver")
        return None, None
    except Exception as exc:
        print(f"  {ROOD}Verbindingsfout: {exc}{RESET}")
        print(f"  Controleer: sudo docker ps | grep cassandra")
        return None, None


def tel_totaal(session):
    """Telt het totale aantal stemmen via COUNT(*)."""
    try:
        rij = session.execute(
            f"SELECT COUNT(*) FROM {CASSANDRA_TABLE}"
        ).one()
        return rij[0] if rij else 0
    except Exception as exc:
        print(f"  {ROOD}Query fout: {exc}{RESET}")
        return 0


def tel_per_land(session):
    """Telt het aantal stemmen per land."""
    per_land = {}
    try:
        for land in LANDEN:
            rij = session.execute(
                f"SELECT COUNT(*) FROM {CASSANDRA_TABLE} "
                f"WHERE country = %s ALLOW FILTERING",
                [land]
            ).one()
            per_land[land] = rij[0] if rij else 0
    except Exception as exc:
        print(f"  {ROOD}Per land query fout: {exc}{RESET}")
    return per_land


def voeg_teststemmen_in(session, aantal):
    """
    Voegt exact 'aantal' teststemmen in Cassandra in.
    Zelfde formaat als insert_vote.py.
    """
    ingevoegd = 0
    try:
        for _ in range(aantal):
            session.execute(
                f"INSERT INTO {CASSANDRA_TABLE} "
                f"(vote_id, country, timestamp, voter_ip) "
                f"VALUES (%s, %s, %s, %s)",
                (
                    uuid.uuid4(),
                    random.choice(LANDEN),
                    datetime.now(),
                    f"192.168.{random.randint(0,255)}.{random.randint(1,254)}"
                )
            )
            ingevoegd += 1
    except Exception as exc:
        print(f"  {ROOD}Fout bij invoegen: {exc}{RESET}")
    return ingevoegd


# ================================================================
# CHECK 2 - DATA INTEGRITEIT
# ================================================================

def check_integriteit(session, n_stemmen, simulatie=True):
    """
    Verificatie punt 5 - Check 2: Data integriteit.

    Werkwijze:
      1. Tel stemmen VOOR het invoegen (baseline)
      2. Voeg exact n_stemmen in
      3. Tel stemmen NA het invoegen
      4. Controleer: NA - VOOR = n_stemmen
    """
    print(f"\n{VET}Check 2: Data integriteit{RESET}")
    print(f"  Methode: tel VOOR invoegen, voeg {n_stemmen} in, tel NA invoegen")
    print(f"  Query  : SELECT COUNT(*) FROM {CASSANDRA_TABLE};")
    print(f"  NFR    : geen dataverlies (NFR-03)")
    print()

    if simulatie:
        import random
        voor     = 500
        na       = voor + n_stemmen
        # per_land reflecteert het totaal na invoegen (na = 1500)
        per_land = {}
        resterend = na
        for i, land in enumerate(LANDEN):
            if i == len(LANDEN) - 1:
                per_land[land] = resterend
            else:
                deel = int(na * random.uniform(0.06, 0.14))
                deel = min(deel, resterend)
                per_land[land] = deel
                resterend -= deel
    else:
        # Stap 1: tel VOOR
        voor = tel_totaal(session)
        print(f"  Stemmen VOOR invoegen : {voor:>10,}")

        # Stap 2: voeg in
        print(f"  Stemmen invoegen      : {n_stemmen:>10,}  (wacht even...)")
        ingevoegd = voeg_teststemmen_in(session, n_stemmen)
        print(f"  Effectief ingevoegd   : {ingevoegd:>10,}")

        # Stap 3: tel NA
        na       = tel_totaal(session)
        per_land = tel_per_land(session)

    verschil = na - voor
    geslaagd = (verschil == n_stemmen)

    print(f"  Stemmen NA invoegen   : {na:>10,}")
    print(f"  Verschil (NA - VOOR)  : {verschil:>10,}")
    print(f"  Verwacht              : {n_stemmen:>10,}")

    if geslaagd:
        print(f"\n  {GROEN}{VET}GESLAAGD{RESET}")
        print(f"  {n_stemmen} stemmen ingevoegd, {verschil} gevonden in Cassandra")
        print(f"  NFR-03 voldaan: geen dataverlies")
    else:
        verschil_abs = abs(n_stemmen - verschil)
        print(f"\n  {ROOD}{VET}GEFAALD{RESET}")
        print(f"  {verschil_abs} stemmen verloren of teveel")
        print(f"  NFR-03 NIET voldaan")

    # Toon telling per land
    if per_land:
        print(f"\n  {'Land':<15} {'Stemmen':>10}")
        print(f"  {'-'*15} {'-'*10}")
        for land in sorted(per_land, key=lambda x: per_land[x], reverse=True):
            print(f"  {land:<15} {per_land[land]:>10,}")

    return geslaagd, voor, na, verschil, per_land


# ================================================================
# CHECK 3 - INTERNE CONSISTENCY CHECK
# ================================================================

def check_consistentie(totaal_globaal, per_land):
    """
    Verificatie punt 5 - Check 3: Interne consistency check.

    Vergelijkt COUNT(*) globaal met de som van landen apart.
    Als die twee gelijk zijn: data is intern consistent.
    """
    print(f"\n{VET}Check 3: Interne consistency check{RESET}")
    print(f"  Vergelijkt COUNT(*) globaal met som van landen apart")
    print()

    som_landen = sum(per_land.values())
    verschil   = abs(totaal_globaal - som_landen)
    consistent = (verschil == 0)

    print(f"  COUNT(*) globaal  : {totaal_globaal:>10,}")
    print(f"  Som per land      : {som_landen:>10,}")
    print(f"  Verschil          : {verschil:>10,}")

    print(f"\n  {'Land':<15} {'Count(*)':>10} {'% van totaal':>14}")
    print(f"  {'-'*15} {'-'*10} {'-'*14}")
    for land in sorted(per_land, key=lambda x: per_land[x], reverse=True):
        pct = (per_land[land] / totaal_globaal * 100) if totaal_globaal > 0 else 0
        print(f"  {land:<15} {per_land[land]:>10,} {pct:>13.1f}%")

    if consistent:
        print(f"\n  {GROEN}{VET}GESLAAGD{RESET}")
        print(f"  Globaal totaal = som per land = {totaal_globaal:,}")
        print(f"  Data is intern consistent in Cassandra")
    else:
        print(f"\n  {ROOD}{VET}GEFAALD{RESET}")
        print(f"  Verschil van {verschil:,} stemmen")

    return consistent, totaal_globaal, som_landen


# ================================================================
# RAPPORT
# ================================================================

def rapport_opslaan(resultaten):
    """Slaat het rapport op als JSON bestand."""
    ts   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    naam = f"controle_rapport_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    data = {
        "project"    : "Eurovision Mega Data Challenge",
        "student"    : "Mohamed Amin Ahkim",
        "timestamp"  : ts,
        "modus"      : "echte pipeline" if USE_REAL_CASSANDRA else "simulatie",
        "verificatie": "Punt 5",
        "cassandra"  : f"{CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE}",
        "resultaten" : resultaten,
    }
    with open(naam, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"\n  Rapport opgeslagen: {naam}")
    return naam


# ================================================================
# HOOFDFUNCTIE
# ================================================================

def main():
    global USE_REAL_CASSANDRA

    parser = argparse.ArgumentParser(description="Eurovision Controle Script")
    parser.add_argument("--real", action="store_true", help="Echte Cassandra")
    parser.add_argument("--n",    type=int, default=1000,
                        help="Aantal teststemmen om in te voegen (standaard: 1000)")
    args = parser.parse_args()

    if args.real:
        USE_REAL_CASSANDRA = True

    simulatie = not USE_REAL_CASSANDRA

    print(f"\n{VET}Eurovision Stemsysteem   Controle en Audit{RESET}")
    print(f"{'=' * 60}")
    print(f"Verificatie  : Punt 5   checks 2 en 3")
    print(f"Modus        : {'Echte Cassandra' if not simulatie else 'Simulatie'}")
    print(f"Teststemmen  : {args.n:,} stemmen worden ingevoegd")
    print(f"Cassandra    : {CASSANDRA_HOST}:{CASSANDRA_PORT} / "
          f"{CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE}")
    print(f"{'=' * 60}")

    # Cassandra verbinding
    session = cluster = None
    if not simulatie:
        session, cluster = cassandra_verbinding()
        if session is None:
            return

    # Check 2
    check2_res = check_integriteit(session, args.n, simulatie)
    check2_ok, voor, na, verschil, per_land = check2_res

    # Check 3 (gebruik NA als globaal totaal)
    check3_ok, glob, som = check_consistentie(na, per_land)

    # Verbinding sluiten
    if cluster:
        cluster.shutdown()

    # Eindrapport
    print(f"\n{'=' * 60}")
    print(f"{VET}EINDRAPPORT   Verificatie punt 5{RESET}")
    print(f"{'=' * 60}")

    resultaten = {
        "check_2_integriteit": {
            "geslaagd"       : check2_ok,
            "voor_invoegen"  : voor,
            "na_invoegen"    : na,
            "ingevoegd"      : args.n,
            "gevonden"       : verschil,
            "query"          : f"SELECT COUNT(*) FROM {CASSANDRA_TABLE}",
        },
        "check_3_consistentie": {
            "geslaagd"   : check3_ok,
            "globaal"    : glob,
            "som_landen" : som,
            "methode"    : "COUNT(*) globaal vs som per land",
        }
    }

    for naam_check, data in resultaten.items():
        kleur  = GROEN if data["geslaagd"] else ROOD
        status = "GESLAAGD" if data["geslaagd"] else "GEFAALD"
        print(f"  {naam_check:<30}: {kleur}{VET}{status}{RESET}")

    if check2_ok and check3_ok:
        print(f"\n  {GROEN}{VET}ALLE VERIFICATIECHECKS GESLAAGD{RESET}")
        print(f"  NFR-03 voldaan: {args.n} stemmen ingevoegd, {verschil} gevonden")
        print(f"  Data intern consistent in Cassandra")
    else:
        print(f"\n  {ROOD}{VET}EEN OF MEER CHECKS GEFAALD{RESET}")

    rapport_opslaan(resultaten)
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    main()
