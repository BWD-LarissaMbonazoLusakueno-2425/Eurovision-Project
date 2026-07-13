Adam Bouchikhi (r1048050)     Arsalan Borna (r1035349)    Larissa Mbonazo Lusakueno (r1029256)       Mohamed Amin Ahkim (r1038196) Saartje Cordier (r1037117)        

 **Auditsysteem voor Eurovision contest**                                                       


# Inhoudsopgave

[**Inhoudsopgave 2**](#inhoudsopgave)

[**Voorwoord 4**](#voorwoord)

[**Inleiding 5**](#inleiding)

> [Context van de opdracht 5](#context-van-de-opdracht)
>
> [Probleemstelling 5](#probleemstelling)
>
> [Doelstelling 5](#doelstelling)
>
> [Afbakening 6](#afbakening)

[**Analyse 6**](#analyse)

> [Requirements 6](#requirements)
>
> [Functioneel 6](#functioneel)
>
> [Niet-functioneel 6](#niet-functioneel)
>
> [Architectuur 6](#architectuur)
>
> [Pipeline overzicht 6](#pipeline-overzicht)
>
> [Datacenter & cloud 8](#datacenter-cloud)
>
> [Componenten 8](#componenten)
>
> [Data model 8](#data-model)
>
> [Technologische keuzes 9](#technologische-keuzes)
>
> [Inleiding 9](#inleiding-1)
>
> [Apache Kafka 9](#apache-kafka)
>
> [Apache Spark 9](#apache-spark)
>
> [Apache Cassandra 9](#apache-cassandra)
>
> [Apache Hadoop 10](#apache-hadoop)
>
> [Docker 10](#docker)
>
> [Virtuele machines en cloudomgeving
> 10](#virtuele-machines-en-cloudomgeving)
>
> [Waarom een cloudomgeving? 11](#waarom-een-cloudomgeving)

[**Proces 11**](#proces)

> [Aanpak & planning 11](#aanpak-planning)
>
> [Werkwijze 11](#werkwijze)
>
> [Tools 12](#tools)
>
> [Taakverdeling 12](#taakverdeling)

[**Implementatie 12**](#implementatie)

> [Overzicht implementatie 12](#overzicht-implementatie)
>
> [Pipeline ontwerp 13](#pipeline-ontwerp)
>
> [Data ingestie (Kafka) 13](#data-ingestie-kafka)
>
> [Verwerking (Spark) 13](#verwerking-spark)
>
> [Opslag (Cassandra) 14](#opslag-cassandra)
>
> [Cloud setup en infrastructuur 14](#cloud-setup-en-infrastructuur)
>
> [Technische uitwerking 16](#technische-uitwerking)
>
> [Opzetten van de virtuele machine
> 16](#opzetten-van-de-virtuele-machine)
>
> [Installatie en configuratie van Docker
> 17](#installatie-en-configuratie-van-docker)
>
> [Opzetten van Kafka, Spark en Cassandra
> 17](#opzetten-van-kafka-spark-en-cassandra)
>
> [Uitvoeren van de pipeline 18](#uitvoeren-van-de-pipeline)
>
> [Problemen en oplossingen 18](#problemen-en-oplossingen)
>
> [Testing & Validatie 19](#testing-validatie)
>
> [Testaanpak 19](#testaanpak)
>
> [Uitgevoerde testen 19](#uitgevoerde-testen)
>
> [Analyse van resultaten 19](#analyse-van-resultaten)
>
> [Resultaten 20](#resultaten)
>
> [Beperkingen 21](#beperkingen)

[**Reflectie 21**](#reflectie)

> [Teamreflectie 21](#teamreflectie)
>
> [Individuele reflectie 21](#individuele-reflectie)

[**Referenties 24**](#referenties)

[**Bijlagen 24**](#bijlagen)

# 

# 

# 

# 

# 

# Voorwoord

Dit verslag werd opgesteld in het kader van het vak Integration Project
I binnen de opleiding Toegepaste Informatica aan Odisee Hogeschool
Brussel gedurende het academiejaar 2025-2026.

Het project heeft als doel een proof of concept te ontwikkelen van een
schaalbaar auditsysteem voor de verwerking van stemmen tijdens de
Eurovision Song Contest. Hiervoor werd gebruikgemaakt van verschillende
Big Data-technologieën zoals Apache Kafka, Apache Spark, Apache
Cassandra, Hadoop, Docker en Google Cloud Platform. Door middel van een
gedistribueerde architectuur werd onderzocht hoe grote hoeveelheden
stemdata op een betrouwbare, performante en schaalbare manier kunnen
worden verwerkt.

Tijdens dit project hebben wij niet alleen onze technische kennis verder
ontwikkeld, maar ook ervaring opgedaan in het samenwerken binnen een
projectteam, het oplossen van complexe technische problemen en het
documenteren van een volledige software- en infrastructuuroplossing.

Wij willen graag onze begeleidende docent, Yvan Rooseleer, bedanken voor
zijn begeleiding, feedback en ondersteuning gedurende het project. Zijn
advies heeft ons geholpen bij het maken van technische keuzes en het
verder uitwerken van onze oplossing.

Wij hopen dat dit verslag een duidelijk beeld geeft van de gemaakte
keuzes, de gebruikte technologieën, de uitgevoerde implementatie en de
behaalde resultaten binnen dit project.

# 

# Inleiding

## Context van de opdracht

Het is nog nooit zo belangrijk geweest om grote hoeveelheden data te
kunnen verwerken in een beperkte tijd. Een klassiek voorbeeld waarbij
dit onmiddellijk duidelijk wordt is de Eurovision Song Contest. Hier
wordt in korte tijd een enorme datastroom gegenereerd afkomstig uit vele
landen.

Zonder een correcte en efficiënte verwerking is het onmogelijk om tot
een betrouwbare ranking te komen. Maar zo'n evenementen genereren
piekbelastingen die traditionele systemen gewoonweg niet aankunnen en
dus zijn schaalbare, performante oplossingen noodzakelijk.

Wij gaan in dit project een proof of concept ontwikkelen voor een
systeem dat deze stemdata via een gedistribueerde pipeline kan
verwerken. Hiervoor onderzoeken en gebruiken we technieken zoals Apache
Kafka, Apache Spark en Apache Cassandra.

## Probleemstelling

De Eurovision Song Contest verwerkt jaarlijks een enorm grote
hoeveelheid stemmen afkomstig uit verschillende landen. Deze stemmen
worden in een korte tijd verzameld en moeten snel en correct verwerkt
worden. Om zo tot een definitieve ranking te komen.

Het verwerken van deze grote datastroom is een uitdaging. Traditionele,
niet-geschaalde oplossingen kunnen het lastig krijgen met de zware
belasting, dit kan vertragingen en fouten in de verwerking tot gevolg
hebben. Dit is dan ook de reden dat het noodzakelijk is om een oplossing
te zoeken die schaalbaar, betrouwbaar en efficiënt is.

Boven op de verwerking moet het systeem ook controles kunnen uitvoeren
om de juistheid van de gegevens te kunnen aantonen. Dit zorgt dan weer
voor een hogere complexiteit. Een goed ontworpen, gedistribueerde aanpak
is dus noodzakelijk.

## Doelstelling

Het doel van dit project is het ontwikkelen van een proof of concept
(PoC) voor een schaalbaar systeem dat in staat is om de stemmen van de
Eurovision Song Contest correct te verwerken en een juiste ranking op te
stellen.

De focus van ons project ligt op de volgende punten:

- verwerking van de stemmen via een gedistribueerde architectuur

- berekenen van een concrete en consistente ranking

- uitvoeren van controles

Het is niet de bedoeling dan we een volledig afgewerkt product
afleveren, maar wel dat we een prototype maken om aan te tonen dat het
zou werken in een realistische omgeving.

## Afbakening

Aan het begin van het project werden er een aantal duidelijke afspraken
gemaakt:

- stemmen zijn enkel in SMS-formaat

- 1 SMS = 1 stem, er wordt geen gebruik gemaakt van het puntensysteem

- het systeem berekent het aantal stemmen per land

- er wordt een ranking opgesteld

- het is een passief systeem dat als controle werkt langs het officiële

# Analyse

## Requirements

Deze hebben we afgeleid uit de analyse en doelstellingen.

### Functioneel

het systeem moet:

- F-R01 - stemdata kunnen opslaan

- F-R02 - inkomende stemmen kunnen verwerken

- F-R03 - een correcte ranking berekenen op basis van stemmen

- F-R04 - de resultaten van de stemming kunnen weergeven

- F-R05 - controles kunnen uitvoeren

- F-R06 - nieuwe stemmen blijven verwerken terwijl het systeem actief is

- F-R07 - een werkende pipeline hebben

### Niet-functioneel

Het systeem moet:

- NF-R01 - schaalbaar zijn voor grote hoeveelheden data

- NF-R02 - performant zijn voor snelle berekeningen

- NF-R03 - betrouwbare en consistente resultaten opleveren

- NF-R04 - robuust zijn en blijven werken bij hoge belasting

- NF-R05 - flexibel zijn om makkelijk uit te kunnen breiden

- NF-R06 - beschikbaar blijven tijdens de hele stemperiode

- NF-R07 - efficiënt omgaan met resources

## Architectuur

### Pipeline overzicht

Het systeem dat we hebben ontwikkeld is gebouwd als een pipeline. Met
andere woorden, de data die wordt ontvangen stroomt stap voor stap
doorheen ons systeem en wordt bij elke stap een beetje meer verwerkt.

Het begint met de inkomende stemmen die worden omgezet naar een
standaardformaat om de verwerking makkelijker te maken. Hierna worden ze
doorgestuurd naar Kafka, een soort buffer die ervoor zorgt dat er geen
stemmen verloren gaan.

Dan begint de verwerking van de data in real-time door Apache Spark. Het
aantal stemmen wordt geteld per land en er wordt een ranking berekend.
Deze verwerking is continu, het blijft doorgaan terwijl er nieuwe
stemmen binnenkomen.

Na de verwerking worden de resultaten opgeslagen in Cassandra. In een
uitgebreidere versie kan Hadoop gebruikt worden voor het archiveren van
data.

Ten slotte worden de resultaten weergegeven in een ranking. Deze kan dan
vergeleken worden met de officiële ranking.

*Figuur 1: Overzicht van de data pipeline voor het verwerken van
stemmen, van inkomende SMS tot opslag en
controle*![](media/image6.png){width="4.677431102362204in"
height="5.527872922134733in"}

### Datacenter & cloud

Ook de infrastructuur van het systeem is belangrijk om even bij stil te
staan. Het systeem draait namelijk niet op één enkele computer maar
bestaat uit verschillende onderdelen bestaande uit virtuele machines en
containers.

We maakten gebruik van een virtuele machine waarop verschillende tools
zijn geïnstalleerd. In de VM maken we gebruik van containers om de
verschillende componenten, zoals Kafka, Spark en Cassandra afzonderlijk
te laten draaien.

Dan maakten we een meer uitgebreide opstelling waarbij het systeem kan
worden verdeeld over meerdere datacenters en servers. Dit spreidt de
verwerking waardoor het systeem veel beter bestand is tegen hoge
belasting. Deze soort opstelling zorgt ervoor dat meerdere instanties
van bijvoorbeeld Kafka of Spark tegelijkertijd kunnen draaien, wat de
schaalbaarheid vergroot.

De architectuur kan ook in een cloudomgeving draaien, zoals via Google
Cloud. Wij gebruiken hier verschillende virtuele machines die
verschillende onderdelen van het systeem hosten. Dit zorgt voor een
hogere flexibiliteit en schaalbaarheid.

### Componenten

Het systeem bestaat uit verschillende belangrijke componenten die elk
een specifieke rol hebben:

- Apache Kafka

> Dit wordt gebruikt als een soort buffer die de inkomende datastroom
> bijhoudt. Wat ervoor zorgt dat er geen data verloren gaat of corrupt
> wordt. Het draagt bij aan een vlotte verwerking.

- Apache Spark

> Hier gebeurt de verwerking, filtering en telling van de stemmen. Hier
> wordt de ranking bepaald.

- Apache Cassandra

> Dit slaat de actuele data op. De telling wordt hierin bewaard. Dit
> komt omdat Cassandra geschikt is voor snelle verwerking

- Hadoop

> Het wordt gebruikt als opslag voor historische data. Hadoop kan
> gebruikt worden voor de opslag van grotere hoeveelheden data, deze
> kunnen later geanalyseerd worden.

- Docker

> Dankzij docker kunnen we alle verschillende onderdelen in containers
> draaien. Dit ondersteunt een consistente werking en zorgt ervoor dat
> alles makkelijk te implementeren is.

## Data model

In dit project werken we met stemdata. Elke stem wordt opgeslagen als
een aparte record in ons systeem. Het datamodel is simpel in elk record
worden de volgende dingen bijgehouden:

- vote_id

> Een unieke identifier per stem. Dit vermijdt duplicaten.

- country

> Dit toont voor welk land gestemd werd. Dit wordt gebruikt om de
> stemmen te tellen en de ranking op te stellen.

- timestamp

> Dit veld toont wanneer gestemd werd. Dat is belangrijk zijn voor
> real-time verwerking en latere analyse.

- voter_ip

> Het IP-adres van de stemmer kan gebruikt worden voor controle en
> validatie van de stemmen.

Deze structuur maakt het mogelijk om later makkelijk beperkingen uit te
voeren, wanneer de data verder door de pipeline gaan. Zo kunnen alle
stemmen worden verwerkt en kan er een ranking worden berekend.

## Technologische keuzes

### Inleiding

Na ons onderzoek bleek al snel dat dit project schaalbaarheid en
efficiëntie van groot belang waren. Het systeem moet grote hoeveelheden
data in real-time kunnen verwerken. Dit is dan ook waarom we in de
wereld van big data zijn gekropen en hier verschillende technologieën
hebben onderzocht en gekozen om verder te gebruiken.

Deze gekozen technologieën vormen de basis van ons gehele project en
zorgen ervoor dat de verwerking van de stemmen correct en performant kan
verlopen.

### Apache Kafka

Apache Kafka wordt gebruikt als datastreaming platform. Kafka zal de
inkomende stemmen opvangen en tijdelijk bufferen.

Tijdens de stemming kunnen plots grote pieken ontstaan. Dankzij Kafka
zal deze data niet verloren gaan en steeds aan een stabiel tempo kunnen
blijven verwerkt worden. Het is een soort tussenlaag tussen de input en
de verwerking van de data.

### Apache Spark

Apache Spark wordt gebruikt voor dataverwerking. Het zal de stemmen in
real-time analyseren en verwerken.

Spark zal de data uit Kafka uitlezen en hierop bewerkingen uitvoeren,
zoals het tellen van de stemmen en het berekenen van de ranking. Spark
heeft de mogelijkheid om data parallel te verwerken, dit maakt het
ideaal om grote hoeveelheden data snel te verwerken.

### Apache Cassandra

Apache Cassandra is de database waar de resultaten worden opgeslagen.
Het is een NoSQL database dat is ontworpen om grote hoeveelheden data
met hoge snelheden te verwerken.

Wij gebruiken Cassandra om de verwerkte stemresultaten op te slaan.

Cassandra maakt gebruik van een peer-to-peer architectuur waarbij geen
centrale master aanwezig is. Elke node kan lees- en schrijfbewerkingen
verwerken. Hierdoor ontstaat geen Single Point of Failure en blijft het
systeem beschikbaar wanneer een node uitvalt.

De data wordt verdeeld via partitionering over meerdere nodes. Indien
gewenst kan replicatie worden toegepast zodat dezelfde gegevens op
meerdere nodes worden opgeslagen.

### Apache Hadoop

Apache Hadoop wordt gebruikt om historische gegevens op te slaan. In
tegenstelling tot Cassandra, kan Hadoop grote datasets op lange termijn
bewaren.

Hadoop maakt het mogelijk om achteraf analyses of controles uit te
voeren op de volledige dataset.

### Docker

Docker wordt gebruikt om de verschillende onderdelen van het systeem in
containers te draaien. Hierdoor kunnen alle componenten in een
geïsoleerde omgeving werken zonder dat er conflicten ontstaan tussen
verschillende softwareversies.

Docker maakt het eenvoudiger om het systeem op te starten en te
gebruiken, zowel lokaal als in de cloud. Tegelijkertijd zorgt het ook
voor netwerkisolatie. Door middel van een Docker bridge netwerk konden
Kafka, Spark en Cassandra elkaar bereiken via hun containernamen. Dit
alles maakte de configuratie en de onderlinge communicatie eenvoudiger.

### Virtuele machines en cloudomgeving

In de ontwikkeling en initiële testing fase maakten we gebruik van
virtuele machines die lokaal bij iedereen waren geïnstalleerd. Zeker in
deze fasen was het een groot voordeel dat deze een gecontroleerde
omgeving boden waarin we al onze tools veilig konden draaien. Later zijn
we overgegaan naar een cloudomgeving.

Tijdens de onderzoeksfase werd eerst gekeken naar het gebruik van Oracle
Cloud in combinatie met KVM virtualisatie. De beperkingen van onze
studentenaccounts hinderde ons echter om deze omgeving correct te kunnen
valideren.

Daarom hebben we uiteindelijk gekozen voor Google Cloud. Hier zijn we
erin geslaagd de nodige virtuele machines succesvol op te zetten. In
deze gesimuleerde productieomgeving konden we een realistischer systeem
opbouwen en de verschillende onderdelen van de pipeline beter testen.

### Waarom een cloudomgeving?

We kozen hiervoor omdat het eenvoudig meerdere virtuele machines kan
aanbieden binnen één netwerk. Hierdoor konden Kafka, Spark en Cassandra
op afzonderlijke VM\'s draaien. Daarnaast biedt Google Cloud
flexibiliteit om resources zoals CPU, geheugen en opslag eenvoudig uit
te breiden wanneer de belasting toeneemt.

# Proces

## Aanpak & planning

Wij kozen ervoor om in fasen te werken.

In de eerste fase focusten wij op analyse en verwerking. We onderzochten
het probleem en de mogelijke technologieën en architecturen die we
konden gebruiken om grote hoeveelheden data te verwerken. Vervolgens
hebben we ons verdiept in de gekozen technologieën. Dit vormde de basis
voor de verdere uitwerking van ons project.

Vervolgens heeft iedereen een PoC uitgewerkt in een lokale omgeving.
Hier focusten we ons puur op de basis, namelijk het verwerken van de
stemmen en het genereren van een correcte ranking. Dit gaf ons de kans
om onze aanpak uit te testen zonder deze al te ver uit te werken.

In een volgende stap hebben we ervoor gekozen om het systeem uit te
breiden en aan te passen naar een schaalbaardere en realistischere
omgeving. Om de verwerking efficiënter en flexibeler te maken hebben we
dan ook gebruik gemaakt van containerinstanties en cloudtechnologieën.

Ten slotte hebben we ons gefocust op het verfijnen en testen van het
systeem. Hierbij focusten wij ons vooral op de correctheid, performantie
en betrouwbaarheid.

De planning bestaat uit de volgende fasen:

- analyse en voorbereiding

- ontwikkeling van de PoC

- uitbreiding van de architectuur

- testen en evalueren

## Werkwijze

We hebben gebruikgemaakt van een iteratieve aanpak. Dit zorgde ervoor
dat we regelmatig konden bijsturen. Aangezien het doel was om een
schaalbaar systeem te ontwikkelen voor het verwerken van stemmen, konden
wij het project opdelen in een aantal logische stappen. Deze bouwden
telkens op elkaar voort.

We hebben regelmatig geëxperimenteerd met verschillende oplossingen. Dit
maakt het mogelijk om fouten en problemen vroeg op te sporen, hierdoor
konden we regelmatig bijsturen. Elke teamlid deed individuele
experimenten waardoor we de beste resultaten op alle vlakken konden
samenbrengen tot de optimale oplossing.

Tijdens het hele traject zaten we ook op regelmatige basis samen met
zowel onze begeleider als ook met ons team. Dit gebeurde praktisch
wekelijks. Tijdens de momenten met de begeleider kregen we feedback en
advies. Dit zorgde ervoor dat we op de goede weg bleven en snel vooruit
gingen. Tijdens onze onderlinge meetings verdeelden we de taken en
evolueerden we hoever iedereen zat. Het was dan ook meestal tijdens deze
meetings dat het duidelijk werd wanneer iemand hulp nodig had. Hier werd
dan gekeken hoe we de problemen best konden oplossen en of het nodig was
om een extra persoon op de taak te zetten.

## Tools

Tijdens de ontwikkeling van het project werden verschillende tools en
technologieën gebruikt:

- Apache Kafka = voor het verwerken en bufferen van inkomende
  datastromen

- Apache Spark = voor real-time data-analyse en verwerking

- Apache Cassandra = voor het opslaan van verwerkte gegevens

- Docker = voor het draaien van componenten in containers

- Virtuele machines = voor een geïsoleerde ontwikkelomgeving

- Google Cloud = voor het opzetten van een schaalbare infrastructuur

- Python = voor het simuleren van stemdata

We stelden ook een masterdocument op om een overzicht van alle
technische informatie te behouden.Dit hebben we later verwerkt in de
bestanden die ook in bijlage terug te vinden zijn.

## Taakverdeling

- Adam Bouchikhi

  - Technologieën

- Arsalan Borna

  - Pipeline diagram

- Larissa Mbonazo Lusakueno

  - Verificatie / DevOps

- Mohamed Amin Ahkim

  - Automatisering

- Saartje Cordier

  - Tuning / Kafka producer

# Implementatie

## Overzicht implementatie

In dit project hebben we een proof of concept ontwikkeld, dit om aan te
tonen dat het systeem in staat is om stemmen te verwerken en een
correcte ranking te bepalen.

We hebben de implementatie opgebouwd als een pipeline bestaande uit
verschillende componenten die samenwerken. Elke stap heeft een
specifieke taak:

- binnenhalen van data

- verwerken van data

- opslaan van de resultaten

Het doel is niet om een volledig werkend productiesysteem te bouwen,
maar om te bewijzen dat de gekozen architectuur en technologieën correct
functioneren

## Pipeline ontwerp

De pipeline vormt de kern van het systeem. Hij beschrijft hoe de data
door de verschillende componenten stroomt. Eerst worden de stemmen
gegenereerd, dan wordt de data via Kafka doorgestuurd naar Spark voor
verwerking. Daarna worden de resultaten opgeslagen in Cassandra.

De verwerking kan dankzij deze structuur continu gebeuren en het systeem
blijft schaalbaar. De verschillende onderdelen kunnen onafhankelijk van
elkaar werken en eenvoudig uitgebreid worden, dit allemaal dankzij de
pipeline.

## Data ingestie (Kafka)

De simulatie van inkomende stemmen werd gedaan aan de hand van een
Python-script. Dit genereert continu nieuwe data. Het bootst het
stemgedrag van de gebruikers na en zorgt voor een continue datastroom.

Deze gegenereerde stemmen worden doorgestuurd naar Apache Kafka. Kafka
is de buffer tussen de input en de verwerking die ervoor zorgt dat zelfs
tijdens grote pieken alle stemmen worden opgevangen zonder data te
verliezen.

Kafka maakt het mogelijk om de data op een gestructureerde manier door
te sturen naar Apache Spark.

## Verwerking (Spark)

De verwerking van de data gebeurt met Apache Spark. De data zal worden
uitgelezen uit Kafka en in realtime verwerkt.

De stemmen zullen gesorteerd worden per land vooraleer ze worden
opgeteld. Op basis van deze gegevens wordt de ranking weergegeven die
toont welke landen de meeste stemmen hebben ontvangen.

Een groot voordeel van Spark is de mogelijkheid om data parallel te
verwerken. Aangezien dit ervoor zorgt dat er grote hoeveelheden data
binnen een beperkte tijd kunnen worden verwerkt.

## ![](media/image7.png){width="6.299660979877515in" height="3.3333333333333335in"}

*Figuur 2: Output van het verwerkingsscript waarbij stemgedrag wordt
geanalyseerd en een winnaar wordt bepaald*

## Opslag (Cassandra)

De resultaten worden na de verwerking opgeslagen in Apache Cassandra.
Deze database kan grote hoeveelheden data wegschrijven in een korte
tijd.

Elke stem wordt opgeslagen als een record met verschillende gegevens
zoals een unieke identifier, het land waarop gestemd is, het tijdstip en
het IP-adres van de stemmer.

## Cloud setup en infrastructuur

Om de samenwerking te vergemakkelijken, alsook het systeem realistischer
te maken hebben we de pipeline getest in een cloudomgeving met meerdere
virtuele machines.

Initieel leed ons onderzoek ons naar het gebruik van Oracle Cloud in
combinatie met KVM virtualisatie. We slaagden er echter niet in om deze
omgeving correct te valideren met onze studentenaccounts.

Daarom zijn we overgestapt op Google Cloud, hierin konden we meerdere
virtuele machines opzetten. Hier konden we de verschillende componenten
van het systeem gescheiden draaien, wat beter aansluit bij een
gedistribueerde setup.

![](media/image9.png){width="6.299660979877515in"
height="4.194444444444445in"}

Deze opstelling bewijst de geschiktheid van het systeem om verder te
worden uitgebreid. Wat bewijst dat het systeem geschikt is voor een
grotere infrastructuur

![](media/image2.png){width="6.299660979877515in"
height="6.777777777777778in"}

*Figuur 3: Overzicht van virtuele machines in Google Cloud, verdeeld
over meerdere componenten en datacenters*

## Technische uitwerking

### Opzetten van de virtuele machine

Tijdens de ontwikkelingsfase hebben we een virtuele machine opgezet met
het besturingssysteem Ubuntu. We lieten deze lokaal draaien om zo een
geïsoleerde en gecontroleerde omgeving te creëren.

Dankzij deze omgeving was het mogelijk om alle nodige tools te
installeren zonder invloed te hebben op het hostsysteem. Daardoor is het
veilig om te experimenteren en fouten op te lossen.

![](media/image8.png){width="6.299660979877515in"
height="3.0972222222222223in"}

*Figuur 4: Virtuele machine met Ubuntu waarin de ontwikkelomgeving voor
het project werd opgezet*

### Installatie en configuratie van Docker

Nadat we de virtuele omgeving hadden opgezet, hebben we Docker
geïnstalleerd. Dit om de verschillende componenten in het systeem te
laten draaien.

Door het gebruik van containers was het mogelijk om Kafka, Cassandra en
Spark onafhankelijk van elkaar op te starten. Dit heeft als gevolg dat
de configuratie eenvoudiger werd en alle onderdelen correct konden
samenwerken.

De correcte werking van Docker werd door ons na de installatie
gecontroleerd door de actieve containers te
bekijken.![](media/image1.png){width="7.568484251968504in"
height="0.4898698600174978in"}

*Figuur 5: Overzicht van draaiende Docker containers voor de
verschillende componenten zoals Kafka, Spark en Cassandra*

### Opzetten van Kafka, Spark en Cassandra

Dan werd het tijd om de belangrijkste componenten van de pipeline op te
zetten. Eerst Apache Kafka, dit werd geconfigureerd om de inkomende
stemmen te kunnen ontvangen. Vervolgens Apache Spark, voor de verwerking
van de data. Als laatste Apache Cassandra, Dit werd geïnstalleerd als
database om de resultaten te kunnen opslaan.

Hier was het belangrijk om een goede verbinding op te zetten tussen de
verschillende componenten. De data moest namelijk vlot kunnen
doorstromen.

### Uitvoeren van de pipeline

Na de configuratie werd de pipeline getest door het systeem effectief
uit te voeren. Door middel van een Python-script werden stemmen
gesimuleerd en doorgestuurd naar Kafka. Het uitlezen van deze stemmen
gebeurde dan door Spark, het verwerkte de real-time data tot een
ranking. Als laatste werden de resultaten opgeslagen in Cassandra.

Dit toont de correcte samenwerking van onze verschillende onderdelen in
ons systeem aan en bewijst dat het in staat is om data van input tot
output te verwerken.

![](media/image5.png){width="6.299660979877515in" height="3.125in"}

*Figuur 6: Uitvoering van de data pipeline waarbij grote hoeveelheden
stemdata verwerkt worden en de performantie wordt weergegeven in
gebeurtenissen per seconde*

### Problemen en oplossingen

Tijdens de uitwerking van ons project kwamen er een aantal problemen
naar voren.

Belangrijk was het probleem met het opzetten van een cloudomgeving.
Tijdens de onderzoeksfase probeerde we nog om gebruik te maken van
Oracle Cloud in combinatie met KVM virtualisatie. De correcte validatie
is jammer genoeg nooit geslaagd door beperkingen met onze
studentenaccounts.

Dit hebben we opgelost door te kiezen voor Google Cloud. Hier konden we
meerdere virtuele machines opzetten wat bijdroeg aan een realistischere
omgeving.

Tijdens de ontwikkeling bleek dat de initiële pipeline te traag was. De
verwerking van 1 miljoen stemmen duurde 47 minuten, wat te lang was. De
verschillende optimalisatie pogingen die we hebben ondernomen hadden
geen effect omdat het een hardware probleem was.

Daarom hebben we uiteindelijk de hardware configuratie van Cassandra
aangepast naar een krachtigere VM (4CPU's en 16GB RAM). Ook de code van
het Python-script, demo_integriteit.py, werd geoptimaliseerd door de
concurrency naar 200 te verhogen. Dit verminderde de verwerkingstijd tot
12 minuten voor één miljoen stemmen.

Er zijn ook verschillende kleine technische uitdagingen geweest.
Voorbeelden hiervan zijn de correcte configuratie tussen Kafka en Spark
en het debuggen van fouten tijdens de verwerking. Maar door onze
iteratieve werkwijze konden we deze fouten oplossen en het hele proces
stap voor stap verbeteren.

## Testing & Validatie

### Testaanpak

Uiteraard moest de werking van het volledige systeem gevalideerd worden,
dit deden we aan de hand van een hele hoop gerichte testen. Zo konden we
nagaan of de verschillende componenten goed samenwerkten en of de
verwerking van de stemmen betrouwbaar verloopt.

Door middel van gesimuleerde data konden testen worden uitgevoerd over
de volledige pipeline. Hiermee konden verschillende scenario's worden
nagebootst. Zo konden we de correctheid van de verwerking testen, alsook
de stabiliteit en performantie bij grote hoeveelheden data.

Tijdens de laatste testen was het mogelijk om één miljoen stemmen te
verwerken in 12 minuten.

### Uitgevoerde testen

**Functionele testen:**

- verwerken van inkomende stemmen

- correct doorsturen van data via Kafka

- berekenen van de juiste ranking in Spark

- opslaan van de resultaten in Cassandra

**Performantietesten:**

Dit werd getest door een hoge stroom aan data te genereren, zo konden we
controleren of het systeem stabiel bleef werken.

**Betrouwbaarheid en continuïteit:**

Het was ook nodig om na te gaan of het systeem blijft functioneren
wanneer er continu nieuwe stemmen blijven binnenkomen.

### Analyse van resultaten

Uit de testen trekken wij de conclusie dat de pipeline correct
functioneert. De verwerking van de stemmen en omzetting naar de ranking
verloopt correct. Het systeem lijkt schaalbaar te zijn om grote
hoeveelheden data te kunnen verwerken zonder veel vertraging. Deze
resultaten zijn zichtbaar in de uitvoer van de pipeline zoals
weergegeven in het vorige hoofdstuk.

Hoewel de testen werden uitgevoerd in een gesimuleerde omgeving zijn wij
van mening dat de resultaten een duidelijk beeld geven over het
potentieel om dit systeem in een realistische situatie in te zetten. Het
is duidelijk dat de gekozen technologieën en architectuur geschikt zijn
voor de verwerking van deze grote datastromen.

  ---------------------------------- ---------------
  **Test**                           **Resultaat**
  Kafka ontvangt stemmen             Geslaagd
  Spark verwerkt stemmen             Geslaagd
  Cassandra opslag                   Geslaagd
  Ranking berekening                 Geslaagd
  Verwerking van 1.000.000 stemmen   12 minuten
  Cloud deployment op Google Cloud   Geslaagd
  ---------------------------------- ---------------

## Resultaten

We zijn erin geslaagd een werkend proof of concept te ontwikkelen dat in
staat is stemdata te verwerken via een pipeline. Het systeem ontvangt
inkomende stemmen en buffert deze in Kafka tot Spark ze kan verwerken.
Er wordt dan een correcte ranking opgesteld op basis van het aantal
stemmen per land. Tot slot worden de resultaten opgeslagen in Cassandra.
De pipeline is in staat om continu nieuwe inkomende data te blijven
verwerken.

De testen bewijzen dat het systeem grote hoeveelheden data in een korte
tijd kan verwerken Dit dankzij het gebruik van Apache Kafka en Spark die
ervoor zorgen dat meerdere events per seconde kunnen worden verwerkt.

De opstelling van de verschillende virtuele machines in een
cloudomgeving toont aan hoe schaalbaar het systeem is. De capaciteit van
het systeem kan makkelijk verhoogd worden door resources toe te voegen.

  ------------------------ ----------
  **Situatie**             **Tijd**
  Oorspronkelijke setup    47 min
  Geoptimaliseerde setup   12 min
  Verbetering              74,5%
  ------------------------ ----------

Door het verhogen van de Cassandra resources en het optimaliseren van de
concurrency werd de verwerkingstijd met ongeveer 74,5% gereduceerd.

## 

## Beperkingen

Het is belangrijk om te begrijpen dat dit een proof of concept is, wat
een aantal beperkingen met zich meebrengt.

De data die verwerkt wordt is door ons gesimuleerd en dus niet afkomstig
van een echte bron. Hoewel we het systeem grondig getest hebben, was dit
op kleine schaal en dus niet met een realistisch aantal stemmen voor
tijdens de echte wedstrijd.

Sommige controles, zoals de validatie van stemmen, zijn slechts beperkt
in ons systeem verwerkt. Dit zijn geen grote, abnormale beperkingen voor
een PoC. In de toekomst kunnen deze beperkingen altijd worden
uitgebreid.

# Reflectie

## Teamreflectie

Dit project heeft ons veel bijgeleerd op technisch vlak maar ook op het
gebied van samenwerking. Wat vooral opviel was het belang van
communicatie tussen de teamleden. Omdat iedereen aan verschillende delen
werkte, was het heel belangrijk dat er goed werd gecommuniceerd over
veranderingen en problemen. Wanneer er iemand vastliep, waren de anderen
altijd bereid om te helpen. Dit droeg bij aan een goede samenwerking.

Ook het belang van goede documentatie kwam soms naar boven. Doordat we
met zoveel verschillende technologieën werkten met allemaal
verschillende configuraties en scripts was er een hele hoop technische
documentatie. Dit constant wijzigen bij de kleinste aanpassing bleek
moeilijk om vol te houden. Hierdoor kwam de documentatie niet altijd
meer overeen met de werkelijkheid wat voor problemen kon zorgen
aangezien niet iedereen aan dezelfde delen werkte maar ze wel met elkaar
in verbinding stonden.

Op technisch vlak hebben we veel kennis opgedaan. We hebben met veel
nieuwe technologieën leren werken die een merendeel van ons nog nooit
gebruikt hadden. Ook hebben we deze technologieën en alles wat erbij
komt kijken, leren troubleshooten. We moesten samen problemen
onderzoeken en oplossingen testen. Hierdoor hebben we nog een hele hoop
extra dingen bijgeleerd. We kijken tevreden terug naar dit resultaat en
nemen deze ervaringen mee naar de toekomst.

## Individuele reflectie

**Larissa:**

Dit project heeft me het belang van drie zaken doen inzien. Ten eerste
is goede documentatie essentieel: het houdt de evolutie van het project
traceerbaar, vergemakkelijkt de samenwerking en bewaart het overzicht op
onze doelen. Ten tweede is samenwerken een continu leerproces. Het
vereist een permanente, flexibele communicatiestijl omdat transparantie
niet altijd vanzelfsprekend is. Ten derde heb ik gemerkt dat puur
technische vaardigheden niet volstaan. De echte uitdaging en kracht ligt
in het vertalen van die techniek naar een begrijpelijk businessverhaal
voor mensen zonder IT-achtergrond, wat mij enorm heeft geïnspireerd voor
de toekomst.

**Saartje:**

Dit project heeft mij geholpen om de theoretische onderwerpen die kort
aan bod kwamen tijdens verschillende lessen praktisch te leren gebruiken
en combineren. Het heeft mij ook nogmaals aangetoond hoe belangrijk
duidelijke communicatie is zeker wanneer er vijf teamleden zijn. Dit is
extreem belangrijk om verwarring te voorkomen maar ook om ervoor te
zorgen dat bij problemen anderen kunnen helpen.

Als iemand problemen ondervond dan waren de andere teamleden altijd
bereid om hulp te bieden. Dit zorgde voor een goede sfeer in het team,
wat het project ten goede kwam. Het enige dat soms voor wat frustratie
kon zorgen was de technische documentatie. Als iemand een kleine
aanpassing maakte werd dit niet altijd opgeschreven maar wanneer dit een
paar keer het geval was klopte de documentatie niet meer. Om deze reden
zou ik de volgende keer nog meer hameren op het goed documenteren van
alle technische aspecten.

**Adam:**

Ik ben tevreden over het eindresultaat van dit project. Samen met het
team zijn we erin geslaagd een volledige data pipeline op te bouwen voor
de verwerking en validatie van stemmen. Tijdens het project hebben we
verschillende Big Data-technologieën geïntegreerd, waaronder Apache
Kafka, Apache Spark, Apache Cassandra, Hadoop HDFS, Docker en Google
Cloud Platform. Hierdoor heb ik niet alleen meer inzicht gekregen in de
afzonderlijke technologieën, maar ook in hoe deze samenwerken binnen een
gedistribueerde architectuur.

Mijn belangrijkste bijdrage lag bij het opzetten en configureren van
Cassandra en Hadoop op Google Cloud. Het is gelukt om beide systemen
correct te laten functioneren binnen de infrastructuur en te integreren
met de rest van de pipeline. Daarnaast heb ik veel bijgeleerd over
Docker, cloudinfrastructuur, Hadoop HDFS, netwerken en gedistribueerde
systemen.

Tijdens het project heb ik verschillende concrete problemen ondervonden.
Zo had ik problemen met SSH-toegang via IAP door ontbrekende rechten,
netwerkproblemen op VM's zonder external IP of Cloud NAT,
poortconflicten met bestaande Docker-containers (zoals Cassandra op
poort 9042), fouten bij Hadoop doordat de DataNode niet correct
verbonden was met de NameNode, en vertragingen door tunneling tijdens
het testen van de pipeline. Daarnaast kostte het tijd om de juiste
firewallregels te configureren en om fouten in Docker Compose op te
lossen. Ook het correct laten samenwerken van alle componenten binnen
een gedistribueerde omgeving bleek uitdagender dan aanvankelijk
verwacht.

Wat beter kon, was het documenteren van configuraties en het vooraf
plannen van de infrastructuur. Hierdoor hadden sommige problemen sneller
opgelost kunnen worden en had het team efficiënter kunnen samenwerken.
Ondanks deze uitdagingen heb ik veel technische kennis opgedaan en een
beter inzicht gekregen in hoe schaalbare data pipelines in de praktijk
worden ontworpen, geïmplementeerd en beheerd. Dit project heeft mij niet
alleen technisch sterker gemaakt, maar heeft ook mijn vaardigheden op
vlak van probleemoplossend denken, zelfstandig werken en samenwerken
binnen een technisch project verder ontwikkeld.

**Arsalan:**

Tot nu toe ben ik tevreden over hoe mijn deel van het project is
verlopen. Het is gelukt om Spark correct te verbinden met Cassandra
binnen de pipeline en de stemmen op een betrouwbare manier te verwerken
en te tellen. Ik heb veel bijgeleerd over Apache Spark, de
Spark-Cassandra connector, Docker-netwerken en hoe een verweringslaag
past binnen een grotere Big Data architectuur.

Tijdens het project ben ik wel tegen verschillende concrete problemen
aangelopen. Zo kreeg ik in het begin de foutmelding \"Connection
refused: 127.0.0.1:9042\" omdat Spark naar localhost verwees in plaats
van naar het juiste Cassandra-adres, wat ik heb opgelost door het
hostadres correct te configureren naar de juiste containernaam.
Daarnaast ontbrak in eerste instantie de spark-cassandra-connector, wat
resulteerde in een ClassNotFoundException, en moest ik het juiste
package meegeven bij het uitvoeren van spark-submit. Ook had ik
netwerkproblemen tussen de Spark- en Cassandra-containers, wat ik heb
opgelost door een eigen Docker-netwerk aan te maken en de juiste
containernamen te gebruiken. Verder kostte het tijd om Spark Structured
Streaming volledig werkend te krijgen, waardoor we voor de Proof of
Concept uiteindelijk gekozen hebben voor een directe verwerking als
functioneel equivalent.

Wat beter had gekund, is dat ik de Spark-configuratie en de gebruikte
packages beter had kunnen documenteren tijdens het proces, in plaats van
pas nadat de problemen al opgelost waren. Ook had ik vooraf meer kunnen
testen met Structured Streaming, zodat dit volledig geïntegreerd had
kunnen worden in de pipeline. Ondanks deze uitdagingen heb ik veel
inzicht gekregen in hoe dataverwerking met Spark werkt binnen een
gedistribueerd systeem, en hoe belangrijk een correcte netwerk- en
connectorconfiguratie is voor een stabiele pipeline.

**Mohamed:**

Dit project was een goede leerervaring voor mij. Ik was verantwoordelijk
voor de automatisering, wat betekende dat ik scripts moest schrijven om
de hele pipeline automatisch te laten draaien, de load te testen en de
data te controleren.

Wat ik het meeste heb geleerd is hoe belangrijk het is om goed te kijken
naar wat er al bestaat in het project. In het begin schreef ik scripts
die niet overeenkwamen met de namen die mijn teamleden gebruikten, zoals
verkeerde topicnamen of kolomnamen. Dat zorgde voor problemen die ik
daarna moest oplossen. Ik heb geleerd dat je altijd eerst goed moet
afstemmen met je team voordat je begint.

Ik heb ook geleerd dat testen in de echte omgeving heel anders is dan
testen op je eigen computer. Dingen die lokaal werkten, deden het soms
niet op de VM. Daarom was het belangrijk om alles live te testen.

Samenwerken in een groep was soms uitdagend, maar we hebben het goed
gedaan. Iedereen had zijn eigen rol en dat maakte het overzichtelijk.

# Referenties

Anthropic. (2026). *Claude AI* (Claude 3.5).
[[https://claude.ai]{.underline}](https://claude.ai)

Apache Software Foundation. (z.d.). *Apache Spark documentation*.
[[https://spark.apache.org/docs/latest]{.underline}](https://spark.apache.org/docs/latest)

Google Cloud. (z.d.). *Compute Engine documentation*.
[[https://cloud.google.com/compute/docs]{.underline}](https://cloud.google.com/compute/docs)

Google Cloud. (z.d.). *Google Cloud networking overview*.
[[https://cloud.google.com/networking]{.underline}](https://cloud.google.com/networking)

Microsoft. (2026). *Microsoft Copilot* (Copilot AI).
[[https://copilot.microsoft.com]{.underline}](https://copilot.microsoft.com)

OpenAI. (2026). *ChatGPT* (GPT‑4.1).
[[https://chat.openai.com]{.underline}](https://chat.openai.com)

# Bijlagen

(Deze zijn ook terug te vinden in de map deliverables)

**Bijlage A:** [[Video - Finale
demo]{.underline}](https://drive.google.com/file/d/1OQ-81b_U3kOZy92GlXao2UPhmLaOzlQc/view?usp=sharing)

Video-opname van een volledig werkende pipeline van begin tot eind.

**Bijlage B:** [[Configuratie en Tools - Oracle
box]{.underline}](https://docs.google.com/document/d/1p9j8Ms52BYEB9hqTLJ1r4m3FRE2ofcRwM3vH5HULwpw/edit?usp=sharing)

Korte documentatie van de installatie van de Ubuntu-VM, Python-omgeving
en basisontwikkeltools.

**Bijlage C:** [[Spark en Docker - Oracle
Box]{.underline}](https://docs.google.com/document/d/1pnrcM3ArXgK8AnGFSqjmB9z7r-cLvRUhL0hPyz-f1Gc/edit?usp=sharing)

Beknopte beschrijving van de installatie en de eerste tests van Spark,
Docker en Jupyter binnen de Oracle-VM.

**Bijlage D:** [[Google Cloud Implementatiehandleiding &
VM-configuratie]{.underline}](https://docs.google.com/document/d/1CRnGesY_vwtx496xXJBzIY50EAVWkk2oA9ebBvz8TLY/edit?usp=sharing)

Overzicht van de stappen voor het aanmaken en configureren van de VM's
in Google Cloud.

**Bijlage E:** [[Google Cloud Infrastructuur &
Containerconfiguratie]{.underline}](https://docs.google.com/document/d/1SzqI7zd-tA7LHW86lGy44TprqXJXDa4ECDT7PpOze8g/edit?usp=sharing)

Deze bijlage bevat een volledig overzicht van de infrastructuur die werd
opgezet binnen Google Cloud Platform voor het uitvoeren van de
gedistribueerde pipeline.

**Bijlage F:** [[Pipeline
Cassandra-Hadoop]{.underline}](https://docs.google.com/document/d/1KlFNnJy_v5OMnjjDJtoamSsmASF60nuJRDvgSCMojq8/edit?usp=sharing)

Korte beschrijving van de volledige audit-pipeline, inclusief stappen,
throughput-resultaten en Cassandra-Hadoop-consistentiecontrole.

**Bijlage G:** [[Data Integriteit
documentatie]{.underline}](https://docs.google.com/document/d/1rG3mrVgmcjia_NMgGH00wacVNrFcwZHz0kDD6HNPSb0/edit?usp=sharing)

Beknopte samenvatting van de NFR-03 dataverlies-test en de evolutie van
de verschillende scriptversies.

**Bijlage H:** [[Load testing
documentatie]{.underline}](https://docs.google.com/document/d/1Y0sU8WQJIVIsiJOIuhSqlji4yxfMeEthSqYL-sAU5HU/edit?usp=sharing)

Korte samenvatting van de NFR-02 loadtest, inclusief
throughput-resultaten en stabiliteit van Kafka, Spark en Cassandra onder
piekbelasting.

**Bijlage I:** Deliverables-map

Map met alle scripts, outputs, txt-bestanden en een afbeelding.


Gerealiseerd door

Adam Bouchikhi (r1048050)     Arsalan Borna (r1035349)    Larissa Mbonazo Lusakueno (r1029256)       Mohamed Amin Ahkim (r1038196) Saartje Cordier (r1037117) 
Tweede Bachelor Toegepaste Informatica Odisee
Academiejaar 2025-2026
