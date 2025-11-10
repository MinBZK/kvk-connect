**Referentie**: https://developers.kvk.nl/cms/api/uploads/KVK_Catalogus_KMS_Mutatieservice_versie_1_2.pdf

## Inhoudsopgave

1. [Overzicht Signalen](#overzicht-signalen)
2. [Signaal Nieuwe Inschrijving](#signaal-nieuwe-inschrijving)
3. [Signaal Gewijzigde Inschrijving](#signaal-gewijzigde-inschrijving)
4. [Signaal Gewijzigde Vestiging](#signaal-gewijzigde-vestiging)
5. [Signaal Beëindiging](#signaal-beëindiging)
6. [Signaal Rechtsvormwijziging](#signaal-rechtsvormwijziging)
7. [Signaal VoortzettingEnOverdracht](#signaal-voortzettingenoverdracht)

## Overzicht signalen

| Signaal                          | Globale signaalbeschrijving                                                       |
|----------------------------------|-----------------------------------------------------------------------------------|
| Nieuwe Inschrijving              | Treedt op als er een nieuw KVK-nummer wordt geregistreerd in het Handelsregister. |
| Gewijzigde Inschrijving          | Treedt op als bij een bestaande organisatie wijzigingen plaatsvinden.             |
| Gewijzigde Vestiging             | Treedt op als wijzigingen plaatsvinden bij vestigingen.                           |
| Signaal Beëindiging              | Treedt op als een bestaande inschrijving wordt beëindigd of is uitgeschreven.     |
| Signaal Rechtsvormwijziging      | Treedt op als een rechtspersoon zich omzet in een andere rechtsvorm.              |
| Signaal VoortzettingEnOverdracht | Treedt op als er een voortzetting of overdracht heeft plaatsgevonden.             |



## Signaal Nieuwe Inschrijving

**Doel**: Het Signaal Nieuwe Inschrijving betreft het bekendmaken van nieuwe inschrijvingen in het Handelsregister

### Specifieke gegevens van het signaal
In het signaalbericht nieuwe inschrijvingen worden de volgende aanvullende gegevens verstuurd.
Het signaalbericht bevat het gegeven ‘_aanleiding_’.

| Gebeurtenisnaam                   | Code | Aanleiding                |
|-----------------------------------|------|---------------------------|
| Voorzetten vestiging              | VV   | Voortzetten onderneming   |
| Oprichten rechtspersoon           | OR   | Oprichten                 |
| Voortzetten onderneming           | VO   | Voortzetten               |
| Fuseren van rechtspersonen        | FS   | Fuseren                   |
| Splitsen van rechtspersoon        | SP   | Splitsen                  |
| Verplaatsing zetel naar Nederland | ZV   | Zetelverplaatsing         |
| Nieuw ingeschreven rechtspersoon  | NW   | Nieuw                     |

### Signaal Gewijzigde Inschrijving

**Doel**:  Het Signaal Gewijzigde Inschrijving geeft een melding als er een wijziging is in een inschrijving in het
Handelsregister. U ontvangt een signaalbericht van een inschrijving (lees KVK-nummer) wanneer de
gegevens van deze inschrijving in het Handelsregister zijn gewijzigd.

### Specifieke gegevens van het signaal
Een wijziging van een inschrijving in het Handelsregister vindt op één of meerdere gegevens plaats.
In het Signaal Gewijzigde Inschrijving geven we aan welk blok met gegevens (gegevensblok) er is
gewijzigd. Een gegevensblok is een samenhangende set van gegevens, bijvoorbeeld functionaris
gegevens. Het signaal geeft alleen aan welk gegevensblok er is gewijzigd, en niet de gewijzigde
gegevens. Bij een wijziging in een voor u relevant gegevensblok, vraagt u de gegevens op met een
Handelsregisterproduct (bijvoorbeeld KVK API Basisprofiel en/of KVK Dataservice Inschrijving).

De huidige versie van de KVK Mutatieservice geeft de volgende blokken aan:
 • Maatschappelijke activiteit/onderneming
 • Eigenaar
 • Vestiging (0 of meerdere)
 • Functionarissen (0 of meerdere)
 • Deponeringen

## Signaal Gewijzigde Vestiging
**Doel**:  In het Signaal Gewijzigde Vestiging geven we aan welke gegevens (gewijzigde gegevens) voor een
vestiging zijn gewijzigd. In het signaal wordt alleen aangegeven welk gegeven er is gewijzigd, en niet
de inhoud van de wijziging. U kunt de inhoud van de wijziging ophalen via een KVK-product.

### Specifieke gegevens van het signaal

Het Signaal Gewijzigde Vestiging levert de wijziging in het Handelsregister bij een vestiging. Dit signaal
biedt daarmee de mogelijkheid om wijzigingen specifiek op vestigingsniveau van een inschrijving in het
Handelsregister te volgen.

Een wijziging van een vestiging in het Handelsregister vindt op één of meerdere gegevens plaats.
In het Signaalbericht Gewijzigde Vestiging worden de volgende vestigingsdetails en gewijzigde gegevens
verstuurd.

* vestigingDetails
* Vestigingsnummer
* isHoofdvestiging
* IsCommercieel
* gewijzigdeGegevens
* Naamgeving
* Activiteiten
* Adresgegevens
* Gevolmachtigden


## Signaal Beëindiging

**Doel**:  Met het signaal beëindiging is het mogelijk om geïnformeerd te worden over de beëindiging van een
KVK-nummer in het Handelsregister.

In welke situaties wordt het signaal beëindiging verstuurd? Het signaal wordt verstuurd bij:
* opheffing van de eenmanszaak;
* beëindiging / ontbinding van een rechtspersoon;
* beëindiging / ontbinding van een buitenlandse vennootschap;
* beëindiging / ontbinding van een samenwerkingsverband

### Specifieke gegevens van het signaal

| Gebeurtenisnaam                   | Code Aanleiding | Omschrijving Aanleiding  |
|-----------------------------------|-----------------|--------------------------|
| Opheffen onderneming              | OO              | Opheffen onderneming     |
| Beëindigen rechtspersoon          | BR              | Beëindigen rechtspersoon |
| Fuseren van rechtspersonen        | FS              | Fuseren                  |
| Splitsen van rechtspersonen       | SP              | Splitsen                 |
| Voortzetten onderneming           | VZ              | Voortzetten              |
| Overdragen onderneming            | OV              | Overdracht               |
| Zetelverplaatsing naar buitenland | ZV              | Zetelverplaatsing        |
| Beëindiging                       | BE              | Beëindiging              |


## Signaal Rechtsvormwijziging

Doel:  Bij een rechtsvormwijziging wordt een rechtspersoon omgezet in een andere rechtsvorm. Een rechts
vormwijziging wordt ook wel omzetting genoemd

### Specifieke gegevens van het signaal

 Het signaalbericht rechtsvormwijziging bevat de volgende aanvullende gegevens:
* Rechtsvormwijziging
* Rechtsvorm (oude)
* De oude / vorige rechtsvorm. De nieuwe rechtsvorm (actuele situatie) wordt vermeld bij de
standaard signaalgegevens

## Signaal VoortzettingEnOverdracht

**Doel**: U ontvangt het signaal VoortzettingEnOverdracht indien een bestaande onderneming of vestiging is
voortgezet door of overgedragen aan een nieuwe of een bestaande inschrijving. Dit heeft mogelijk
gevolgen voor de overeenkomsten die u met uw relatie heeft afgesloten.

### Specifieke gegevens van het signaal

In het signaalbericht worden de volgende aanvullende gegevens verstuurd:
* Startdatum, ingangsdatum van de voortzetting/overdracht
* Voortzetting en overdracht
* Betrokken vestigingen
* Vestigingsnummer(s)

Het vestigingsnummer die met de voortzetting/overdracht zijn betrokken.
* Rol bij voortzetting/overdracht
* Code
* VG = Voortgezet door
* OG = Overgedragen aan
* Omschrijving

De rol die de inschrijving heeft waar de vestiging ‘naar toe’ is gegaan.
* Overgedragen aan
* Voortgezet door
* KVK nummer

De inschrijving die de onderneming/vestigingen heeft overgenomen/voortzet.
* Rechtsvorm
