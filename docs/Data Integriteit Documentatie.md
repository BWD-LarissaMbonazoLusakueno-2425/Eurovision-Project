Het doel van deze test was om aan te tonen dat onze architectuur voldoet
aan NFR-03, namelijk dat er geen dataverlies mag optreden tijdens het
verwerken van stemmen.

Omdat wij verschillende scripts hebben gaan wij die vergelijken script
staat voor de allereerste script die we gegenereerd hebben met AI-agent
Claude. Alle scripts staan [[eronder]{.underline}](#scripts).

Vergelijkingstabel van alle versies van de

+:--------------------------------+:--------------------------------+:----------------------------------------------------------------+
| [[Eerste                        | [[Tweede                        | [[Laatste                                                       |
| versie]{.underline}](#script-1) | versie]{.underline}](#script-2) | versie]{.underline}](#laatste-versie-demo_final_integriteit.py) |
+---------------------------------+---------------------------------+-----------------------------------------------------------------+
| Nadelen:                        | Verbeteringen:                  | Verbetering:                                                    |
|                                 |                                 |                                                                 |
| - Elke insert wacht op de       | - Prepared statements vermijden | - Gebruik van CONCURRENCY                                       |
|   vorige                        |   dat Cassandra telkens de      |                                                                 |
|                                 |   query moet parsen             | - sequentiële loop werd vervangen door                          |
| - De Python-driver moet         |                                 |   execute_concurrent_with_args()                                |
|   1.000.000 keer een round-trip | - Chunks zorgen voor overzicht  |                                                                 |
|   doen wat extreem traag is     |   en betere foutdetectie        |                                                                 |
|                                 |                                 |                                                                 |
| - Er is geen batching geen      | - De code werd leesbaarder en   |                                                                 |
|   concurrency en geen prepared  |   meer consistent               |                                                                 |
|   statements                    |                                 |                                                                 |
|                                 |                                 |                                                                 |
| -                               |                                 |                                                                 |
+---------------------------------+---------------------------------+-----------------------------------------------------------------+
| Resultaat:                      | Resultaat:                      | Resultaat:                                                      |
|                                 |                                 |                                                                 |
| - extreem traag uitvoeringstijd | - De inserts gebeuren nog       | - Gebruik van echte parallel inserts (150 tegelijk)             |
|                                 |   steeds één voor één binnen    |                                                                 |
| - amper 1.000 inserts/sec       |   elke chunk dus de snelheid    | - Gebruik van prepared statements zorgen voor een vermindering  |
|                                 |   bleef beperkt.                |   van overhead per insert                                       |
| - Uitvoeringstijd van 41        |                                 |                                                                 |
|   minuten                       |                                 | - Uitvoeringstijd van enkele minuten                            |
+---------------------------------+---------------------------------+-----------------------------------------------------------------+

Bijlage

Output van de [[script 1]{.underline}](#script-1):

![](media/image4.png){width="6.267716535433071in"
height="2.9027777777777777in"}

![](media/image1.png){width="6.267716535433071in"
height="6.666666666666667in"}

![](media/image3.png){width="2.1458333333333335in"
height="2.4270833333333335in"}

Output van de [[vierde script]{.underline}](#_6nfpwzml7zm8)

![](media/image5.png){width="6.267716535433071in" height="5.875in"}

![](media/image2.png){width="6.267716535433071in"
height="5.916666666666667in"}

## [[Scripts]{.underline}](#scripts)

### [[Script 1]{.underline}](#script-1)

**gcloud compute ssh dc1-cassandra \--zone=europe-west1-b
\--command=\"cat \> /home/larissa_mbonazolusakueno/data_integriteit.py
\<\< \'EOFSCRIPT\'**

**import uuid, random**

**from datetime import datetime**

**from cassandra.cluster import Cluster**

**LANDEN =
\[\\\"Belgium\\\",\\\"Netherlands\\\",\\\"France\\\",\\\"Germany\\\",\\\"Italy\\\",**

**\\\"Spain\\\",\\\"Sweden\\\",\\\"Norway\\\",\\\"Ukraine\\\",\\\"Portugal\\\"\]**

**N = 1_000_000**

**cluster = Cluster(\[\\\"localhost\\\"\])**

**session = cluster.connect(\\\"eurovision\\\")**

**\# VOOR**

**voor = session.execute(\\\"SELECT COUNT(\*) FROM
votes\\\").one()\[0\]**

**print(f\\\"VOOR : {voor:,} stemmen in Cassandra\\\")**

**print(f\\\"Invoegen: {N:,} stemmen\...\\\")**

**\# INVOEGEN**

**batch_size = 1000**

**ingevoegd = 0**

**for i in range(0, N, batch_size):**

**for \_ in range(min(batch_size, N - i)):**

**session.execute(**

**\\\"INSERT INTO votes (country,timestamp,vote_id,voter_ip) VALUES
(%s,%s,%s,%s)\\\",**

**(random.choice(LANDEN), datetime.now(), uuid.uuid4(),**

**f\\\"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}\\\")**

**)**

**ingevoegd += min(batch_size, N - i)**

**if ingevoegd % 100_000 == 0:**

**print(f\\\" {ingevoegd:,} ingevoegd\...\\\")**

**\# NA**

**na = session.execute(\\\"SELECT COUNT(\*) FROM votes\\\").one()\[0\]**

**verschil = na - voor**

**print(f\\\"NA : {na:,} stemmen in Cassandra\\\")**

**print(f\\\"Verschil: {verschil:,}\\\")**

**print(f\\\"Verwacht: {N:,}\\\")**

**if verschil == N:**

**print(\\\"GESLAAGD - NFR-03 voldaan: geen dataverlies\\\")**

**else:**

**print(f\\\"GEFAALD - {abs(N-verschil):,} stemmen verloren\\\")**

**cluster.shutdown()**

**EOFSCRIPT\"**

### [[Script 2]{.underline}](#script-2)

**gcloud compute ssh dc1-cassandra \--zone=europe-west1-b
\--command=\"cat \> /home/larissa_mbonazolusakueno/demo_integriteit.py
\<\< \'EOFSCRIPT\'**

**import uuid, random**

**from datetime import datetime**

**from cassandra.cluster import Cluster**

**LANDEN =
\[\\\"Belgium\\\",\\\"Netherlands\\\",\\\"France\\\",\\\"Germany\\\",\\\"Italy\\\",**

**\\\"Spain\\\",\\\"Sweden\\\",\\\"Norway\\\",\\\"Ukraine\\\",\\\"Portugal\\\"\]**

**N = 1_000_000**

**print(\\\"=\\\"\*60)**

**print(\\\" Eurovision Audit Systeem - Data Integriteit Test\\\")**

**print(\\\" NFR-03: Geen dataverlies\\\")**

**print(\\\" Odisee Hogeschool - Mega Data Challenge 2025-2026\\\")**

**print(\\\"=\\\"\*60)**

**print()**

**cluster = Cluster(\[\\\"localhost\\\"\])**

**session = cluster.connect(\\\"eurovision\\\")**

**\# STAP 1 - VOOR**

**voor = 0**

**for land in LANDEN:**

**count = session.execute(**

**\\\"SELECT COUNT(\*) FROM votes WHERE country=%s\\\", \[land\]**

**).one()\[0\]**

**voor += count**

**print(f\\\"STAP 1 - Baseline telling:\\\")**

**print(f\\\" Stemmen in Cassandra VOOR test : {voor:\>10,}\\\")**

**print()**

**\# STAP 2 - INVOEGEN**

**print(f\\\"STAP 2 - Exact {N:,} stemmen invoegen\...\\\")**

**start = datetime.now()**

**batch_size = 1000**

**ingevoegd = 0**

**for i in range(0, N, batch_size):**

**for \_ in range(min(batch_size, N - i)):**

**session.execute(**

**\\\"INSERT INTO votes (country,timestamp,vote_id,voter_ip) VALUES
(%s,%s,%s,%s)\\\",**

**(random.choice(LANDEN), datetime.now(), uuid.uuid4(),**

**f\\\"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}\\\")**

**)**

**ingevoegd += min(batch_size, N - i)**

**if ingevoegd % 100_000 == 0:**

**elapsed = (datetime.now() - start).seconds**

**print(f\\\" {ingevoegd:\>9,} ingevoegd \| {elapsed}s verstreken\\\")**

**eind = datetime.now()**

**duur = (eind - start).seconds**

**print(f\\\" Invoegen klaar in {duur} seconden\\\")**

**print()**

**\# STAP 3 - NA (per land)**

**print(f\\\"STAP 3 - Telling per land na invoegen:\\\")**

**print(f\\\" {\'Land\':\<15} {\'Stemmen\':\>10}\\\")**

**print(f\\\" {\'-\'\*15} {\'-\'\*10}\\\")**

**na = 0**

**per_land = {}**

**for land in LANDEN:**

**count = session.execute(**

**\\\"SELECT COUNT(\*) FROM votes WHERE country=%s\\\", \[land\]**

**).one()\[0\]**

**per_land\[land\] = count**

**na += count**

**print(f\\\" {land:\<15} {count:\>10,}\\\")**

**print(f\\\" {\'-\'\*15} {\'-\'\*10}\\\")**

**print(f\\\" {\'TOTAAL\':\<15} {na:\>10,}\\\")**

**print()**

**\# STAP 4 - VERIFICATIE**

**print(f\\\"STAP 4 - Verificatie NFR-03:\\\")**

**verschil = na - voor**

**print(f\\\" Stemmen VOOR : {voor:\>10,}\\\")**

**print(f\\\" Stemmen NA : {na:\>10,}\\\")**

**print(f\\\" Verschil : {verschil:\>10,}\\\")**

**print(f\\\" Verwacht : {N:\>10,}\\\")**

**print()**

**print(\\\"=\\\"\*60)**

**if verschil == N:**

**print(\\\" GESLAAGD - NFR-03 VOLDAAN\\\")**

**print(f\\\" {N:,} stemmen ingevoegd, {verschil:,} gevonden\\\")**

**print(\\\" Geen dataverlies in Cassandra\\\")**

**else:**

**verlies = abs(N - verschil)**

**print(\\\" GEFAALD - NFR-03 NIET VOLDAAN\\\")**

**print(f\\\" {verlies:,} stemmen verloren\\\")**

**print(\\\"=\\\"\*60)**

**print(f\\\" Tijdstip: {datetime.now().strftime(\'%Y-%m-%d
%H:%M:%S\')}\\\")**

**print(\\\"=\\\"\*60)**

**[cluster.shutdown()]{.underline}**

**[EOFSCRIPT\"]{.underline}**

### [[Laatste versie]{.underline}](https://drive.google.com/open?id=1-Jgk3DURc3cqSeZoc4wFT3nUDEJZwdwa) (demo_final_integriteit.py)
