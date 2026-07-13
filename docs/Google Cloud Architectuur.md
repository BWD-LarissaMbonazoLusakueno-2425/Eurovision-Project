#### [[Architectuur in Google Cloud]{.underline}](#architectuur-in-google-cloud)

We hebben 4 logische datacenters (DC1-DC4) die binnen dezelfde GCP-regio
europe-west1 draaien. Elk datacenter bevat exact drie virtuele machines:

- Een Kafka-node

- Een Spark-node

- Een Cassandra-node

Deze drie componenten zijn de bouwstenen van ons pipeline waardoor de
volledige architectuur bestaat uit 11 virtuele machines die identiek
geconfigureerd zijn als e2-medium instanties (2 vCPU, 4GB RAM). Behalve
voor dc1-cassandra (e2-standard 4 vCPU's, 16GB RAM). Alle machines
bevinden zicht in hetzelfde VPC-subnet (10.132.0.0/20).

Hier is een reeks van stappen voor het aanmaken van de eerst datacenter
(het is ons centrale datacenter):

# ![](media/image17.png){width="6.267716535433071in" height="4.875in"}

# ![](media/image12.png){width="6.267716535433071in" height="6.416666666666667in"}

![](media/image8.png){width="6.267716535433071in"
height="5.166666666666667in"}

![](media/image19.png){width="6.267716535433071in" height="5.25in"}

![](media/image20.png){width="6.267716535433071in"
height="5.152777777777778in"}

![](media/image11.png){width="6.267716535433071in"
height="5.138888888888889in"}

# ![](media/image2.png){width="6.267716535433071in" height="5.5in"}

![](media/image10.png){width="6.267716535433071in"
height="5.180555555555555in"}

![](media/image9.png){width="6.267716535433071in"
height="5.138888888888889in"}

![](media/image5.png){width="6.267716535433071in"
height="5.138888888888889in"}

![](media/image22.png){width="6.267716535433071in" height="5.125in"}

![](media/image21.png){width="6.267716535433071in" height="5.125in"}

![](media/image18.png){width="6.267716535433071in" height="5.125in"}

![](media/image14.png){width="6.267716535433071in"
height="5.138888888888889in"}

##### 

##### 

##### 

##### 

##### 

##### 

##### 

##### 

##### 

##### 

##### 

##### 

##### [[Docker installatie]{.underline}](#docker-installatie) 

Op alle 12 virtuele machines hebben we Docker geïnstalleerd.

# ![](media/image16.png){width="6.267716535433071in" height="5.013888888888889in"}

![](media/image7.png){width="6.267716535433071in"
height="2.9305555555555554in"}

# ![](media/image16.png){width="6.267716535433071in" height="5.013888888888889in"}

# ![](media/image13.png){width="6.267716535433071in" height="5.111111111111111in"}

##### 

##### 

##### 

##### 

##### 

##### 

##### 

##### 

##### 

##### 

##### 

##### 

##### 

##### [[Firewall opzetten]{.underline}](#firewall-opzetten)

Het opzetten van een firewall was fundamenteel voor ons project:

- Google cloud blokkeert standaard alle inkomende traffic.

- Onze pipeline is afhankelijk van open poorten ([[zie technisch
  verslag)]{.underline}](https://docs.google.com/document/u/0/d/1SzqI7zd-tA7LHW86lGy44TprqXJXDa4ECDT7PpOze8g/edit)

- Alle 12 virtuele machines zitten op één netwerk (10.132.0.x)

- De load testing scripts sturen 1 miljoen stemmen via Kafka. Zonder de
  firewall zou Kafka geen enkele producer accepteren.

- Spark workers moeten [[master
  bereiken]{.underline}](https://docs.google.com/document/u/0/d/1SzqI7zd-tA7LHW86lGy44TprqXJXDa4ECDT7PpOze8g/edit).
  Zonder de firewall blijven de *workers* op *standalone* en zou de
  cluster niet werken.

- Zonder de firewall zou Spark geen INSERT doen en Cassandra zou dus
  geen stemmen in de database hebben.

# ![](media/image6.png){width="6.267716535433071in" height="7.930555555555555in"}

![](media/image15.png){width="6.267716535433071in"
height="9.069444444444445in"}

Gebruik van interne IP's voor alle VM's behalve voor die van het
centrale datacenter (die met dc1-starten) voor een aantal redenen:

- Het zorgt voor een lage *latency*

- Het is goedkoper en stabieler

- Het zorgt voor meer veiligheid

- Clusters-netwerken beter intern presteren

Wat belangrijk is, want Kafka, Spark en Cassandra moeten in real-time
werken.

Maar de virtuele machines van de centrale datacenter hebben wél externe
ip's nodig want:

- We moeten op die VM's via ssh kunnen inloggen

- Die maken het mogelijk om de Docker logs te kunnen zien

- Die maken het ook mogelijk om de pipeline te starten

- Ook bij debugging en aanpassingen gebruikten wij altijd die virtuele
  machines in combinatie met de cloud shell

- Zonder die extern ip's zou het moeilijk zijn om de producers te
  starten

# ![](media/image4.png){width="6.267716535433071in" height="3.6944444444444446in"}

![](media/image3.png){width="6.267716535433071in"
height="3.6666666666666665in"}

![](media/image1.png){width="6.267716535433071in"
height="3.7083333333333335in"}

#### [[Belangrijke commando's]{.underline}](#belangrijke-commandos)

+:------------------------+:-----------------------------------------------------------------------------------------------------------------------------------+
| alle 12 vm's zien       | gcloud compute instances list                                                                                                      |
+-------------------------+------------------------------------------------------------------------------------------------------------------------------------+
| Cassandra resetten      | # gcloud compute ssh dc1-cassandra \--zone=europe-west1-b \\                                                                       |
|                         |                                                                                                                                    |
|                         | #  \--command=\"sudo docker exec eurovision-cassandra cqlsh -e \\\"TRUNCATE eurovision.votes;\\\"\"                                |
+-------------------------+------------------------------------------------------------------------------------------------------------------------------------+
| producer runnen         | # gcloud compute ssh dc1-kafka \--zone=europe-west1-b \\                                                                           |
|                         |                                                                                                                                    |
|                         | #  \--command=\"sudo docker run \--rm \--network host eurovision-producer\"                                                        |
|                         |                                                                                                                                    |
|                         | #                                                                                                                                  |
+-------------------------+------------------------------------------------------------------------------------------------------------------------------------+
| Spark job               | # gcloud compute ssh dc1-spark \--zone=europe-west1-b \--tunnel-through-iap \\                                                     |
|                         |                                                                                                                                    |
|                         | #  \--command=\"sudo docker run \--rm \--network host eurovision-spark\"                                                           |
+-------------------------+------------------------------------------------------------------------------------------------------------------------------------+
| eindresultaat mini demo | # gcloud compute ssh dc1-cassandra \--zone=europe-west1-b \\                                                                       |
|                         |                                                                                                                                    |
|                         | #  \--command=\"sudo docker exec eurovision-cassandra cqlsh -e \\\"SELECT count(\*) as totaal_stemmen FROM eurovision.votes;\\\"\" |
|                         |                                                                                                                                    |
|                         | #                                                                                                                                  |
+-------------------------+------------------------------------------------------------------------------------------------------------------------------------+
