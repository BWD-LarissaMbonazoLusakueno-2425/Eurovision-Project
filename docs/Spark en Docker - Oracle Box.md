# Installatie

Aangezien Spark een belangrijk onderdeel van het project zal zijn heb ik
dit ook geïnstalleerd.

> pip install pyspark

Hierna was het tijd om Docker te installeren.

> sudo apt install docker.io -y\
> sudo systemctl start docker\
> sudo systemctl enable docker

Dus eerst werd het geïnstalleerd, daarna werd het opgestart en dan heb
ik ingesteld het automatisch bij het booten opstart.

Zoals het misschien al opvalt, momenteel ben ik bijna alles als sudo aan
het doen. Hoewel dit geen groot b-probleem is, zou het makkelijker zijn
als ik dit gewoon als gebruiker kon uitvoeren. Daarom is de volgende
stap de rechten toekennen aan de gebruiker om dit uit te voeren.
Vervolgens moet ik rebooten om te kijken of het gelukt is.

> sudo usermod -aG docker \$USER\
> sudo reboot

Docker testen is helemaal niet
moeilijk:![](media/image3.png){width="5.447916666666667in"
height="0.6326388888888889in"}

## Experimenteren

### Jupyter

Om wat te oefenen heb ik eerst een container aangemaakt met het volgende
commando:

> docker run -it -p 8888:8888 jupyter/pyspark-notebook

Eerst start het een nieuwe container, vervolgens wordt er gezorgd voor
een interactieve terminal en de Docker sessie wordt aan poort 8888
gekoppeld. "jupyter/pyspark-notebook" is een standaard omgeving voor
data science en big data toepassingen. Dit commando creëerde wel het
probleem dat tijdens dat de Docker aan het runnen was er niets meer in
de terminal van de server kon worden geschreven dat is dan ook de reden
waarom ik hier nu een ander commando voor gebruik.

> docker run -d -p 8888:8888 jupyter/pyspark-notebook

Zoals je ziet is het enige verschil dat het -d is in plaats van -it,
maar dit kleine verschil zorgt ervoor dat de Docker in de achtergrond
draait en de terminal dus vrij blijft voor gebruik.

Vervolgens heb ik dit geopend in een browser via de poort 8888 zoals ik
in het commando heb aangegeven. Om dit te laten slagen heb ik nog aan
aantal dingen gedaan. Eerst en vooral heb ik de netwerkadapter op
Bridged Adapter geplaatst. Daarna heb ik het IP-adres van mijn VM
opgevraagd en genoteerd. om dan vervolgens in de browser
[[http://192.168.86.238:8888]{.underline}](http://192.168.86.238:8888/)
te kunnen opzoeken. Het volgende commando dient om het ip-adres op te
vragen:![](media/image4.png){width="5.65625in"
height="3.488888888888889in"}

ip a

Vervolgens kreeg ik deze pagina te zien:

![](media/image5.png){width="6.267716535433071in"
height="6.305555555555555in"}

Ik heb toen in mijn server terminal het token opgevraagd door middel van
de volgende commando's:

docker ps\
docker logs awesome_johnson![](media/image1.png){width="6.29375in"
height="0.6416666666666667in"}

Toen begonnen er een aantal problemen voor te komen. Ik heb
verschillende dingen geprobeerd, maar de enige manier waarop ik het
werkende kreeg was door het paswoord en token volledig weg te halen. Dit
heb ik gedaan door een container op te starten met het commando:

> docker run -d -p 8888:8888 jupyter/pyspark-notebook start-notebook.sh
> \--NotebookApp.token=\'\' \--NotebookApp.password=\'\'
>
> docker run -d \--restart unless-stopped -p 8888:8888 -v
> \~/eurovision_project:/home/jovyan/project jupyter/pyspark-notebook
> start-notebook.sh \--NotebookApp.token=\'\'
> \--NotebookApp.password=\'\'

Hiermee geraakte ik wel op de notebook en was ik in staat op via
jupyter, pandas en spark te oefenen terwijl ik gewoon gekoppeld was aan
de VM. Het eerste commando heb ik al grotendeels uitgelegd, het enige
verschil is het einde dat er simpelweg voor zorgt dat er geen wachtwoord
of token is.

Het tweede commando is ook grotendeels hetzelfde, dit zorgt er gewoon
voor dat ik niet elke keer wanneer de VM opnieuw opstart, ik het vorige
commando opnieuw moet uitvoeren. Het wordt nu automatisch uitgevoerd
elke keer dat de VM opstart. Het zorgt er ook voor dat de map op de VM
wordt gekoppeld aan de map in de container.

### Clipboard

Het bleek ook niet vanzelfsprekend om dingen te kopiëren en plakken
tussen mijn laptop en de VM. Het heeft een tijdje geduurd vooraleer ik
dit heb opgelost.

Eerst en vooral heb ik de instellingen in Oracle Virtualbox zelf
gewijzigd door het clipboard in te stellen als bidirectional. Dit was
helaas niet voldoende om het te doen werken.

![](media/image2.png){width="4.2990146544181975in"
height="2.0780949256342955in"}
