# Gekozen technologieën (van binnenkomen van de date tot resultaten)

1.  Apache Kafka

    a.  Vangt alle stemmen op

    b.  Zorgt dat er geen data verloren gaat

2.  Apache Spark

    a.  Verwerkt alle stemmen die binnenkomen

    b.  Berekend de ranking

3.  Apache Cassandra

    a.  Slaat de actuele telling op (kent de recentste aantallen)

4.  Hadoop

    a.  Bewaart alle historische data (een archief)

# Opzetten technische omgeving

## Virtuele machine

In eerste instantie heb ik een server zonder GUI gebruikt, dit heb ik
redelijk snel aangepast. Ik heb de server verwijderd en een
desktopversie met een GUI geïnstalleerd omdat dit toch wel meer
voordelen heeft. Hoewel de volgende tekst over de installatie van de
serverversie gaat, zijn alle commando's die ik later heb uitgevoerd in
de desktopversie dezelfde. Het enige verschil is dat de ISO-image anders
is.

Om dit project op te zetten heb ik gebruik gemaakt van Oracle
VirtualBox. Ik heb vanop de officiële website van Ubuntu een ISO-image
gedownload voor een Linux server, meer bepaald
ubuntu-22.04.5-live-server-amd64. Ik heb gekozen voor deze versie omdat
momenteel de betrouwbaarste versie lijkt. Het is een LTS-versie wat
ervoor zorgt dat deze over het algemeen stabiel is en nog een tijd
ondersteund wordt. Aangezien het ook niet de nieuwste versie is, is er
een kleinere kans op mogelijke problemen zoals compatibiliteitsproblemen
met bepaalde programma's.

Nadat ik dit bestand heb gedownload op mijn laptop heb ik hem op Oracle
VirtualBox een virtuele machine aangemaakt met deze ISO. Hier heb ik een
aantal dingen ingesteld zoals: RAM, CPU, geheugen, ... Ik heb ervoor
gekozen om een manuele installatie te doen. Vervolgens begon de ISO te
installeren. Hier heb ik nog een aantal dingen ingesteld waaronder een
login en wachtwoord.

Wanneer ik voor het eerst inlogt op de server heb ik de volgende
commando's uitgevoerd om er zeker van te zijn dat de server up-to-date
was.

> sudo apt update\
> sudo apt upgrade -y

Het eerste commando haalt van alles de nieuwste versies op, terwijl het
tweede commando deze versies vergelijkt met de huidige en de nieuwere
versies installeert. Een kleine toelichting voor het tweede commando, -y
wordt gebruikt om direct toestemming te geven zodat dit niet meer moet
worden bevestigd. Dit heeft in elk commando deze betekenis en zal dus
waarschijnlijk vaker terugkomen.

## Ontwikkeltools

Eerst en vooral heb ik python geïnstalleerd, samen met pip en een
virtuele omgeving.

> sudo apt install python3 python3-pip python3-venv -y

Het begint met het installeren van de programmeertaal python. Dan pip,
wat het mogelijk maakt om externe pakketten te installeren (noodzakelijk
voor bijvoorbeeld Pandas). Ten slotte python3-venv, dit is nodig om
virtuele omgevingen te kunnen maken. Als controle heb ik vervolgens de
versies opgevraagd, met onderstaande commando's.

> python3 --version\
> pip3 --version

## Projectmap en python-omgeving

Om het overzicht te bewaren heb ik voor het project een projectmap
aangemaakt.

> mkdir eurovision_project\
> cd eurovision_project

Het eerste dient om de map aan te maken en het tweede om de map te
openen.

Hierna heb ik in deze projectmap een python virtuele omgeving
aangemaakt. Vervolgens heb ik het geactiveerd.

> python3 -m venv venv\
> source venv/bin/activate

Na de activatie ziet de prompt er anders
uit:![](media/image1.png){width="4.444444444444445in"
height="0.22013888888888888in"}
