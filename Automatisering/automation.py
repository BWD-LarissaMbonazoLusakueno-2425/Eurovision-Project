#!/usr/bin/env python3
"""
Eurovision Stemsysteem - Pipeline Automatisering
=================================================
Project  : Mega Data Challenge - Odisee Hogeschool
Rol      : Automatisering
Student  : Mohamed Amin Ahkim

Wat dit script doet:
  1. Controleert of alle Docker containers actief zijn
  2. Voegt teststemmen in via multi_insert.py
  3. Draait de verificatie scripts (load_test + controle)
  4. Genereert een auditrapport

Gebruik
-------
  python3 automation.py           (simulatiemodus)
  python3 automation.py --real    (echte VM)
  python3 automation.py --check   (alleen gezondheidscheck)
"""

import subprocess
import time
import json
import argparse
from datetime import datetime

# ================================================================
# CONFIGURATIE
# ================================================================

USE_REAL = False

WERKMAP  = "~/eurovision"
PYTHON   = "~/eurovision/eurovision_env/bin/python3"

KAFKA_TOPIC    = "votes"
KAFKA_POORT    = 9092
CASSANDRA_POORT = 9042
SPARK_POORT    = 8080
ZK_POORT       = 2181

# ================================================================
# KLEURCODES
# ================================================================

GROEN = "\033[92m"
ROOD  = "\033[91m"
GEEL  = "\033[93m"
BLAUW = "\033[94m"
CYAAN = "\033[96m"
RESET = "\033[0m"
VET   = "\033[1m"
DIM   = "\033[2m"


def log_ok(t):
    print(f"{DIM}[{datetime.now().strftime('%H:%M:%S')}]{RESET} {GROEN}[OK]{RESET}    {t}")

def log_fout(t):
    print(f"{DIM}[{datetime.now().strftime('%H:%M:%S')}]{RESET} {ROOD}[FOUT]{RESET}  {t}")

def log_info(t):
    print(f"{DIM}[{datetime.now().strftime('%H:%M:%S')}]{RESET} {BLAUW}[INFO]{RESET}  {t}")

def log_warn(t):
    print(f"{DIM}[{datetime.now().strftime('%H:%M:%S')}]{RESET} {GEEL}[WARN]{RESET}  {t}")

def log_stap(n, t):
    print(f"\n{VET}{CYAAN}{'=' * 60}{RESET}")
    print(f"{VET}{CYAAN}  STAP {n}: {t}{RESET}")
    print(f"{VET}{CYAAN}{'=' * 60}{RESET}")


# ================================================================
# HULPFUNCTIES
# ================================================================

def run(cmd, timeout=120):
    """Voert een shell commando uit. Geeft (succes, uitvoer, fout) terug."""
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True,
                           text=True, timeout=timeout)
        return r.returncode == 0, r.stdout.strip(), r.stderr.strip()
    except subprocess.TimeoutExpired:
        return False, "", f"Timeout na {timeout}s"
    except Exception as e:
        return False, "", str(e)


def container_actief(naam, simulatie=True):
    """Controleert of een Docker container draait."""
    if simulatie:
        return True
    ok, out, _ = run(
        f"sudo docker ps --filter 'name={naam}' --filter 'status=running' -q"
    )
    return ok and len(out.strip()) > 0


# ================================================================
# STAP 1 - GEZONDHEIDSCHECK
# ================================================================

def stap_gezondheidscheck(simulatie=True):
    """Controleert of alle containers actief zijn."""
    log_stap(1, "Gezondheidscheck")

    services = [
        ("cp-zookeeper",  ZK_POORT,        "Zookeeper"),
        ("cp-kafka",      KAFKA_POORT,      "Apache Kafka"),
        ("cassandra",     CASSANDRA_POORT,  "Apache Cassandra"),
        ("spark-master",  SPARK_POORT,      "Apache Spark"),
    ]

    print()
    print(f"  {'Service':<20} {'Container':<20} {'Poort':<8} Status")
    print(f"  {'-'*20} {'-'*20} {'-'*8} {'-'*10}")

    alle_ok = True
    rapport = []

    for container, poort, naam in services:
        actief = container_actief(container, simulatie)
        status = "ACTIEF" if actief else "INACTIEF"
        kleur  = GROEN if actief else ROOD
        print(f"  {naam:<20} {container:<20} {poort:<8} {kleur}{status}{RESET}")
        rapport.append({"service": naam, "status": status})
        if not actief:
            alle_ok = False

    print()
    if alle_ok:
        log_ok("Alle services zijn actief")
    else:
        log_warn("Niet alle services actief — controleer: sudo docker ps")

    return alle_ok, rapport


# ================================================================
# STAP 2 - TESTSTEMMEN INVOEGEN
# ================================================================

def stap_teststemmen(simulatie=True):
    """
    Voegt teststemmen in via multi_insert.py.
    Dit toont aan dat de pipeline stemmen kan ontvangen en opslaan.
    """
    log_stap(2, "Teststemmen invoegen via multi_insert.py")

    if simulatie:
        log_info("Simulatiemodus: teststemmen worden nagebootst")
        time.sleep(0.5)
        log_ok("Simulatie: stemmen ingevoegd voor alle landen")
        return True

    ok, out, fout = run(f"{PYTHON} {WERKMAP}/multi_insert.py", timeout=30)
    if ok:
        log_ok("Teststemmen ingevoegd in Cassandra")
        log_info(f"  {out}")
    else:
        log_warn(f"multi_insert.py gaf een melding: {fout}")
    return ok


# ================================================================
# STAP 3 - HUIDIG AANTAL STEMMEN TONEN
# ================================================================

def stap_stemmen_tellen(simulatie=True):
    """Toont het huidige aantal stemmen in Cassandra."""
    log_stap(3, "Huidige stemtelling in Cassandra")

    if simulatie:
        log_info("Simulatie: 1.000 stemmen in Cassandra")
        return 1000

    ok, out, fout = run(
        "sudo docker exec eurovision_cassandra_1 cqlsh -e "
        "\"USE eurovision; SELECT COUNT(*) FROM votes;\""
    )

    if ok:
        log_ok(f"Stemtelling opgehaald")
        print(f"\n{out}\n")
    else:
        log_warn(f"Kon stemtelling niet ophalen: {fout}")

    return out


# ================================================================
# STAP 4 - QA SCRIPTS
# ================================================================

def stap_qa_scripts(simulatie=True):
    """
    Draait load_test.py en controle_script.py.
    Gebruikt Python rechtstreeks uit de venv (geen 'source' nodig).
    """
    log_stap(4, "QA en verificatie scripts (punt 5)")

    scripts    = ["load_test.py", "controle_script.py"]
    resultaten = {}

    for script in scripts:
        log_info(f"Script starten: {script}...")

        if simulatie:
            time.sleep(0.5)
            log_ok(f"{script} geslaagd (simulatiemodus)")
            resultaten[script] = {"status": "OK", "uitvoer": "Simulatie"}
        else:
            vlag = "--real" if script == "controle_script.py" else "--real"
            ok, out, fout = run(
                f"cd {WERKMAP} && {PYTHON} {script} {vlag} 2>&1",
                timeout=300
            )
            if ok:
                log_ok(f"{script} geslaagd")
                for regel in out.split("\n")[-10:]:
                    if regel.strip():
                        print(f"    {DIM}{regel}{RESET}")
                resultaten[script] = {"status": "OK", "uitvoer": out[-400:]}
            else:
                log_fout(f"{script} mislukt")
                print(f"    {ROOD}{fout[:200]}{RESET}")
                resultaten[script] = {"status": "FOUT", "uitvoer": fout}

    return resultaten


# ================================================================
# RAPPORT
# ================================================================

def rapport_opslaan(gezondheid, stemmen, qa_res):
    """Slaat een JSON auditrapport op."""
    log_stap("R", "Auditrapport genereren")

    ts   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    naam = f"pipeline_rapport_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    data = {
        "project"   : "Eurovision Mega Data Challenge",
        "student"   : "Mohamed Amin Ahkim",
        "rol"       : "Automatisering",
        "timestamp" : ts,
        "modus"     : "echte VM" if USE_REAL else "simulatie",
        "resultaten": {
            "gezondheidscheck" : gezondheid,
            "stemmen_cassandra": str(stemmen),
            "qa_scripts"       : qa_res,
        }
    }

    with open(naam, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"\n  {VET}Tijdstip :{RESET} {ts}")
    print(f"  {VET}QA resultaten:{RESET}")
    for script, res in qa_res.items():
        kleur = GROEN if res["status"] == "OK" else ROOD
        print(f"    {script:<30}: {kleur}{res['status']}{RESET}")

    log_ok(f"Rapport opgeslagen: {naam}")
    return naam


# ================================================================
# BANNER + MAIN
# ================================================================

def banner(simulatie):
    print(f"\n{VET}{'*' * 60}{RESET}")
    print(f"{VET}  Eurovision Stemsysteem   Pipeline Automatisering{RESET}")
    print(f"{VET}  Odisee Hogeschool   Mega Data Challenge   2025 2026{RESET}")
    print(f"{VET}  Rol: Automatisering   Student: Mohamed Amin Ahkim{RESET}")
    print(f"{VET}{'*' * 60}{RESET}")
    print()
    print(f"  Modus      : {GEEL + 'SIMULATIE' + RESET if simulatie else GROEN + 'ECHTE VM' + RESET}")
    print(f"  Werkmap    : {WERKMAP}")
    print(f"  Kafka topic: {KAFKA_TOPIC}")
    print(f"  Tijd       : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()


def main():
    global USE_REAL

    parser = argparse.ArgumentParser(description="Eurovision Pipeline Automatisering")
    parser.add_argument("--real",  action="store_true", help="Echte VM modus")
    parser.add_argument("--check", action="store_true", help="Alleen gezondheidscheck")
    args      = parser.parse_args()
    simulatie = not args.real

    if args.real:
        USE_REAL = True

    banner(simulatie)

    # Gezondheidscheck
    alle_ok, gezondheid = stap_gezondheidscheck(simulatie)

    if args.check:
        print(f"\n{GROEN}{VET}Gezondheidscheck klaar!{RESET}\n")
        return

    # Teststemmen invoegen
    stap_teststemmen(simulatie)

    # Stemtelling tonen
    stemmen = stap_stemmen_tellen(simulatie)

    # QA scripts
    qa_res = stap_qa_scripts(simulatie)

    # Rapport
    rapport_opslaan(gezondheid, stemmen, qa_res)

    print(f"\n{GROEN}{VET}Pipeline automatisering voltooid!{RESET}\n")


if __name__ == "__main__":
    main()
