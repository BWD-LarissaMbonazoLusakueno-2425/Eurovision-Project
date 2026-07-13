### [[Projectconfiguratie]{.underline}](#projectconfiguratie)

- Project ID: project-c15907ce-0bab-4a45-b70

- Regio: europe-west1

- zone: europe-west1b

- totaal aantal vm's: 12

- VM-type: e2-medium (2 vCPU, 4GB RAM)

uitgezonderd dc1-cassandra (4 vCPU, 8GB RAM)

- Netwerk:

\- Intern subnet: 10.132.0.0/20

\- Externe IP's enkel voor dc1-VM's

\- dc2/dc3/dc4 -\> IAP-tunneling voor SSH

  ----------------- ----------------- ----------------- -------------------
  VM                Intern IP         Extern IP         Rol

  dc1-kafka         10.132.0.5        34.156.36.240     Kafka broker +
                                                        Zookeeper

  dc1-spark         10.132.0.6        34.140.166.209    Spark Master

  dc1-cassandra     10.132.0.7        35.205.105.73     Cassandra + Hadoop
                                                        Namenode/Datanode

  dc2-kafka         10.132.0.9        IAP               Kafka producer node

  dc2-spark         10.132.0.10       IAP               Spark Worker

  dc2-cassandra     10.132.0.8        IAP               Cassandra node

  dc3-kafka         10.132.0.14       IAP               Kafka producer node

  dc3-spark         10.132.0.16       IAP               Spark Worker

  dc3-cassandra     10.132.0.12       IAP               Cassandra node

  dc4-kafka         10.132.0.15       IAP               Kafka producer node

  dc4-spark         10.132.0.17       IAP               Spark Worker

  dc4-cassandra     10.132.0.13       IAP               Cassandra node
  ----------------- ----------------- ----------------- -------------------

Docker containers per vm

#### 

#### [[dc1-cassandra (meest beladen node)]{.underline}](#dc1-cassandra-meest-beladen-node)

  ---------------------- ----------------- ------------ ---------------
  Container              Image             Poorten      Functie
  eurovision-cassandra   cassandra:4.1     9042         database
  eurovision-namenode    hadoop-namenode   9000, 9870   HDFS namenode
  eurovision-datanode    hadoop-datanode   9864         HDFS datanode
  ---------------------- ----------------- ------------ ---------------

#### [[dc1-kafka]{.underline}](#dc1-kafka)

  -------------------------------------- ----------- --------- --------------
  Container                              Image       Poorten   Functie
  larissa_mbonazolusakueno-kafka-1       kafka       9092      Kafka broker
  larissa_mbonazolusakueno-zookeeper-1   zookeeper   2181      Zookeeper
  -------------------------------------- ----------- --------- --------------

#### [[dc1-spark]{.underline}](#dc1-spark)

  ----------------------------------------- ------- ---------------- ---------------------
  Container                                 Image   Poorten          Functie
  larissa_mbonazolusakueno-spark-master-1   spark   7077,7078,8080   Spark master
  larissa_mbonazolusakueno-spark-worker-1   spark   8081             Worker op master-VM
  ----------------------------------------- ------- ---------------- ---------------------

#### [[dc2/dc3/dc4 - Spark workers]{.underline}](#dc2dc3dc4---spark-workers)

  -------------- ------- ---------
  Container      Image   Poorten
  spark-worker   spark   8081
  -------------- ------- ---------

#### [[dc2/dc3/dc4 - kafka producers]{.underline}](#dc2dc3dc4---kafka-producers)

  -------------------------------------- ----------- ---------
  Container                              Image       Poorten
  larissa_mbonazolusakueno-kafka-1       kafka       9092
  larissa_mbonazolusakueno-zookeeper-1   zookeeper   2181
  -------------------------------------- ----------- ---------

#### [[Poorten & Services Overzicht]{.underline}](#poorten-services-overzicht)

  ------- ------------------ ------------------- ----------
  Poort   Service            VM                  Protocol
  9092    Kafka broker       dc1-kafka           TCP
  2181    Zookeeper          dc1-kafka           TCP
  9042    Cassandra CQL      dc1-cassandra       TCP
  7077    Spark Master       dc1-spark           TCP
  7078    Spark Master URL   dc1-spark           TCP
  8080    Spark Master UI    dc1-spark           HTTP
  8081    Spark Worker UI    dc2/dc3/dc4-spark   HTTP
  9000    Hadoop Namenode    dc1-cassandra       TCP
  9870    Namenode UI        dc1-cassandra       HTTP
  9864    Datanode           dc1-cassandra       TCP
  ------- ------------------ ------------------- ----------

Scripts

  --------------------- ------------------------------------ -----------------------------------
  Scriptnaam            Functie                              Locatie
  producer.py           Load testing - stemmen genereren     /home/larissa/producer.py
  demo_integriteit.py   Data-integriteit test (1M stemmen)   /home/larissa/demo_integriteit.py
  demo_consistency.py   Scorebord vs Hadoop check            /home/larissa/demo_consistency.py
  pipeline.sh           Volledige automatisering             \~/pipeline.sh
  --------------------- ------------------------------------ -----------------------------------
