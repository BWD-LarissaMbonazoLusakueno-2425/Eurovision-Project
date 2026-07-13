Het doel van de load test was om aan te tonen dat onze architectuur
voldoet aan NFR-02, namelijk dat het systeem een piekbelasting van
200.000 events per seconde moet kunnen verwerken. De load test moest dus
bewijzen dat onze Kafka-, Spark- en Cassandra-architectuur deze
belasting niet alleen aankan maar ook stabiel blijft onder extreme druk.

### [[STAP 1]{.underline}](#stap-1)

Installatie van Kafka-python en [[producer
script]{.underline}](https://drive.google.com/open?id=1RrdaZgA7LgrQCFxG2Y2wnLwf_KHqYwz2).

![](media/image11.png){width="6.267716535433071in"
height="1.8888888888888888in"}

![](media/image5.png){width="6.267716535433071in"
height="3.4027777777777777in"}

### [[STAP 2]{.underline}](#stap-2)

Instellen van Kafka partities op dc1-kafka. Dit zorgt ervoor dat Kafka
parallel kan werken zodat elke virtuele machine zijn eigen deel van de
load kan verwerken zonder elkaar te blokkeren. Het is ook belangrijk om:

- geen wachtrij te hebben

- een exponentieel stijging in totale throughputte genereren

- data te kunnen repliceren

- brokers te kunnen herstarten zonder dat de pipeline stopt

![](media/image2.png){width="6.267716535433071in"
height="0.6944444444444444in"}

![](media/image6.png){width="6.267716535433071in"
height="1.0972222222222223in"}

#### [[STAP 3]{.underline}](#stap-3)

Daarna hebben wij alle producers tegelijk herstart omdat dit de enige
manier is om een realistische piekbelasting te simuleren en om te testen
of onze architectuur volledig stabiel blijft.

Door alle producers gelijktijdig te herstarten, garanderen we dat:

- Kafka in één keer miljoenen events per seconde moet verwerken

- De totale throughput eerlijk gemeten wordt

- We bottlenecks kunnen identificeren

- We kunnen bewijzen dat de architectuur horizontaal schaalbaar is

- Cassandra onmiddellijk massaal veel writes ontvangt

- Spark streaming onmiddellijk duizenden batches per seconde moet
  verwerken

### [[Test 1]{.underline}](#test-1)

![](media/image10.png){width="6.267716535433071in"
height="2.861111111111111in"}

![](media/image13.png){width="6.267716535433071in"
height="3.4444444444444446in"}

![](media/image3.png){width="6.267716535433071in"
height="2.8333333333333335in"}

![](media/image17.png){width="6.267716535433071in"
height="5.347222222222222in"}

![](media/image4.png){width="6.267716535433071in"
height="4.916666666666667in"}

#### [[Resultaten]{.underline}](#resultaten) 

  --------------- -------------------------
  VM              \% van 200K doel
  dc1-kafka       612% → 1.2M events/sec
  dc1-spark       887% → 1.8M events/sec
  dc1-cassandra   905% → 1.8M events/sec
  dc2-kafka       997% → 2.0M events/sec
  dc2-spark       1017% → 2.0M events/sec
  dc2-cassandra   1428% → 2.9M events/sec
  dc3-kafka       1199% → 2.4M events/sec
  dc3-spark       1019% → 2.0M events/sec
  dc3-cassandra   1105% → 2.2M events/sec
  dc4-kafka       899% → 1.8M events/sec
  dc4-spark       1261% → 2.5 events/sec
  dc4-cassandra   1128% → 2.3M events/sec
  --------------- -------------------------

Totaal samen: 25 miljoen events/sec - 125x het doel van 200K.

### [[Test 2]{.underline}](#test-2)

We hebben ook de verbinding tussen de spark master en de spark workers
getest. Omdat de volledige streaming-architectuur afhankelijk is van een
stabiel Spark Cluster. Tijdens de load testing hebben we het nadruk
gelegd op de verbinding tussen spark master en spark worker want het is
essentieel dat:

- Spark onder extreme load functioneert

- We weten of Spark een bottleneck was of niet

![](media/image8.png){width="6.267716535433071in"
height="0.4722222222222222in"}

![](media/image1.png){width="6.267716535433071in"
height="2.5416666666666665in"}

![](media/image16.png){width="6.267716535433071in"
height="0.4444444444444444in"}

#### Resultaten

  ----------- -------------- --------- ---------
  Worker      IP             Cores     RAM
  dc2-spark   10.132.0.1.0   2 cores   6.8 GIB
  dc3-spark   10.132.0.1.6   2 cores   6.8 GIB
  dc4-spark   10.132.0.1.7   2 cores   6.8 GIB
  ----------- -------------- --------- ---------

### [[Test 3]{.underline}](#test-3)

Wij meten CPU, RAM en netwerk op alle VM's tegelijk tijdens load testing
om te bewijzen dat de volledige gedistribueerde pipeline stabiel blijft
onder extreme piekbelasting en om de echte bottleneck in de architectuur
te identificeren.

![](media/image12.png){width="6.267716535433071in"
height="2.861111111111111in"}

![](media/image14.png){width="6.267716535433071in"
height="4.666666666666667in"}

![](media/image9.png){width="6.267716535433071in"
height="4.777777777777778in"}

![](media/image7.png){width="6.267716535433071in" height="5.375in"}

#### Resultaten

  -------------- -------------------------------------
  CPU verbruik   geen probleem
  RAM            geen probleem
  Netwerk        kafka broker (dc1-kafka bottleneck)
  -------------- -------------------------------------

#### [[Test 4]{.underline}](#test-4)

We hebben de tunneling snelheid gemeten op alle VM's. Omdat:

- De netwerkbandbreedte de maximale throughput van kafka bepaalt

- Spark batches binnen milliseconden moet kunnen ophalen via het netwerk

- Cassandra extreem veel writes moet ontvangen via het netwerk.

![](media/image15.png){width="6.267716535433071in" height="4.25in"}

#### [[Resultaten]{.underline}](#resultaten)

De tunneling beperkt de snelheid vanuit Cloud Shell naar de virtuele
machines maar dit is alleen voor het SSH-beheer (commando's sturen). De
producers zelf communiceren via 10.132.0.x netwerk, wat veel sneller is.
