# KvK-connect

## Inhoudsopgave

1. [Introductie](#introductie)
2. [Vereisten](#vereisten)
3. [Snel aan de slag](#snel-aan-de-slag)
   - [Als bibliotheekgebruiker](#als-bibliotheekgebruiker)
   - [Met Docker](#met-docker)
   - [Als ontwikkelaar](#als-ontwikkelaar)
4. [Structuur van KvK](#structuur-van-kvk)
5. [Data Flow](#data-flow-van-de-docker-apps)
6. [Database Schema](#database-schema)
7. [Functionaliteit](#functionaliteit)
8. [Ontwikkelaarsgids](#ontwikkelaarsgids)
9. [Roadmap](#roadmap)
10. [KvK API Documentatie](#kvk-api-documentatie)

---

## Introductie

De Kamer van Koophandel (KvK) biedt meerdere APIs die samen informatie verstrekken over bedrijven. Uitvoerige documentatie is hier te vinden: https://developers.kvk.nl/documentation

Veel overheidsinstanties hebben een wettelijke taak voor vergunningsverlening, toezicht en handhaving (VTH) en gebruiken deze informatie voor validatie en opslag in hun systemen. Het controleren bij aanvraag gaat vaak met eenmalige KvK-bevragingen, maar signalen dat bedrijven hun statuten wijzigen of ophouden te bestaan zijn ook relevant.

Voor met name de laatste categorie is er synergie te behalen door het eenduidig opzetten van bevragingen en volgen van bedrijfsmutaties. Dit project maakt het bevragen-, opslaan- en werken met deze informatie eenvoudig, eenduidig en deelbaar.
Het idee is dat elke instantie met een eigen KvK subscription op hun eigen infrastructuur in korte tijd een eenvoudig werkende en compliant omgeving heeft opgezet waar direct mee gewerkt kan worden.

---

## Vereisten

> [NB]
> Dit project vereist een KVK API key om te functioneren. Verkrijg een key via [KvK Portaal](https://www.kvk.nl/).
> Voor het volgen van mutaties is een additioneel abonnement nodig.

---

## Snel aan de slag

### Als bibliotheekgebruiker

Installeer het pakket en start direct met KvK-gegevens opvragen:

```bash
pip install kvk-connect
```

```python
from kvk_connect import KVKApiClient, KVKRecordService

client = KVKApiClient(api_key="your_kvk_api_key")
service = KVKRecordService(client)
basisprofiel = service.get_basisprofiel("12345678")
```
**NB:** De package naam is `kvk-connect`, maar imports werken met `kvk_connect` (underscore) in Python.

### Met Docker (stand-alone)

Start een stand-alone lokale instantie met docker compose:

```bash
# Clone en configureer
git clone https://github.com/MinBZK/kvk-connect.git
cd kvk-connect

# Environment instellen
cp .env.docker.example .env.docker
# Bewerk .env.docker met je KvK API-sleutel en alle POSTGRES_* settings, PostgreSQL zal met deze waarden initieren.

# Start services (PostgreSQL + alle apps)
docker compose -f docker-compose.local.yaml up -d

# Check logging met
docker compose logs -f .
```

Op poort 5432 draait nu een PostgreSQL instantie met een `kvkconnect` db zoals uitgelegd onder [Database Schema](#database-schema), deze wordt actueel gehouden door de container-apps, zie [Data Flow](#data-flow-van-de-docker-apps).

### Met Docker (externe Database)

Idem als hierboven, maar gebruik nu de docker-compose.db.yaml file en zet andere variabelen.
```bash
cp .env.docker.example .env.docker
# Bewerk .env.docker met je KvK API-sleutel en SQL Connectie string naar externe DB.

# Start services
docker compose -f docker-compose.db.yaml up -d
```
Voor externe databases moet de database, gebruikersnaam en rechten eerst aangemaakt worden alvorens verbinding gemaakt kan worden.

### Als ontwikkelaar

Clone het project, installeer afhankelijkheden en voer de volledige development workflow uit:

```bash
git clone https://github.com/MinBZK/kvk-connect.git
cd kvk-connect

just install      # Bibliotheek + dev tools installeren
just check-all    # Alle checks uitvoeren (lint, type, tests)
just test         # Tests met coverage
just bump patch   # Versie bumpen
just tag v0.1.5   # Release-tag maken
```

Alle build, lifecycle, run en development-taken zijn als recipes gedefinieerd in `Justfile` als single source of truth. Installeer just en draai `just` zodat je alle gedefinieerde recipes te zien krijgt:

```bash
$ just
Available recipes:
    [deployment]
    build                            # build the distribution packages
    bump tag                         # bump the version in pyproject.toml use: patch, minor, or major
    deploy version                   # publish the package to PyPI
    tag tag msg                      # create and push a git tag use: tag name and message

    [docker]
    docker-build env='local'         # Docker compose up ('local' by default, use 'db' external db build)
    docker-down env='local'          # Docker compose down
    docker-logs env='local' *service # View logs from Docker services
    docker-restart env='local'       # Restart Docker services
    docker-up env='local'

    [lifecycle]
    install                          # First install
    update                           # Update dependencies

    [qa]
    check-all                        # Perform all checks [alias: a]
    cov                              # Run tests and measure coverage
    lint                             # Run linters
    pc                               # Check pre-commit hooks
    test *args                       # Run tests [alias: t]
    typing                           # Check types
```
---

## Structuur KvK
- **Basisprofiel**: Algemene informatie over een bedrijf, zoals naam, KVK nummer, RSIN, oprichtingsdatum, rechtsvorm, eigenaar, etc.
  - Uniek kenmerk: kvk_nummer

- **Vestigingen**: Elke kvkNummer heeft 0 of meer vestigingen.
  - Uniek kenmerk: kvk_nummer met vestigingsnummmer of '0000000' als er geen vestiging bestaat.

- **Vestigingsprofiel**: Informatie over een specifieke vestiging van een bedrijf, zoals post- en bezoek-adres en locatie
  - Uniek kenmerk: vestiging_nummer

- **Signalen**: KvK lijst van mutatie-signalen. Hiermee wordt informatie gegeven welk kvknummer een mutatie heeft verwerkt. De Signaal informatie zelf wordt vooralsnog niet verwerkt. Deze mutaties zijn input voor de 4 apps om nieuwe informatie op te halen.
  - NB: Voor deze mutaties is een losse subscription nodig bij de KvK.

--

## Data Flow van de Docker Apps

De vier Docker apps werken onafhankelijk van elkaar samen om de KVK data actueel te houden:

![AppsStructure](docs/apps.drawio.svg)


### Proces

De apps draaien met `depends_on` ordering:

1. **mutatie-reader** start eerst → pollt KVK Mutaties
2. **basisprofiel** start → verwerkt signalen, haalt bedrijfsgegevens op
3. **vestigingen** start → haalt vestigings-lijsten per bedrijf
4. **vestigingsprofiel** start → haalt adres/postbus per vestiging


## Database Schema (ORM Model)

![ERD](docs/erd.drawio.svg)


### Functionaliteit
* Rate-Limiting
  - We volgen de door KvK gestelde limiet op API calls. Zowel in de kvk-connect library en in docker-compse via een rate limiting gateway.
  - Exponential back-off bij tijdelijke downtime of overschreiden van de rate-limit
* Command Line Interface:
  - Handmatig ophalen van eenmalige informatie.
  - Seeden van de basis profielen middels een CSV file.
* Automatisch volgen van mutatiesignalen middels pull requests.
* Database agnotisch
  - SQLAlchemy ondersteund een lange lijst van DB implementaties: Zie https://docs.sqlalchemy.org/en/21/dialects/index.html. Getest met SQLLite, MS SQL en PostgreSQL
  - We hebben twee docker-compose scripts:
    - 'local': Volledig self contained containers met PostgreSQL
    - 'ext': Zelfde functionaliteit, maar met connectie naar externe database.
* Databases met tabellen worden automatisch aangemaakt, indexen en constraints worden toegevoegd voor optimale werking.

## Roadmap
* Change historie bij kunnen houden.
* HelmChart voor deploy
* PowerBI rapport welke aangesloten kan worden en direct kan werken met de opgehaalde data.



## KVK API Documentatie

### Basisprofiel

| Parameter               | Type   | Details                                                                                                                                                                                                                      |
|-------------------------|--------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| kvkNummer               | string | Dutch KVK number. Consists of 8 digits.                                                                                                                                                                                      |
| indNonMailing           | string | The Company does not wish to receive any unsolicited mail or sales advertising.                                                                                                                                              |
| naam                    | string | Name under societal activity.                                                                                                                                                                                                |
| formeleRegistratiedatum | string | The date the company was registered with the KVK.                                                                                                                                                                            |
| materieleRegistratie    | object | Start date and end date (when applicable) of the company.                                                                                                                                                                    |
| statutaireNaam          | string | The name of the company when articles of association are registered.                                                                                                                                                         |
| handelsnamen            | array  | All names under which a company or branch trades (in order of registration).                                                                                                                                                 |
| sbiActiviteiten         | array  | Code description of SBI activities according to the SBI classification at CBS. There is no maximum number of results. See also www.kvk.nl/sbi. Array contains items with an sbiCode, sbiOmschrijving and indHoofdactiviteit. |
| links                   | array  | 1. Link to the current query 2. Link to all related branches.                                                                                                                                                                |


### Basisprofiel - hoofdvestiging
### Vestigingsprofiel

| Parameter                | Type    | Details                                                                                                                                                                                                                   |
|--------------------------|---------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| vestigingsnummer         | string  | Branch number. Unique number consisting of 12 digits.                                                                                                                                                                     |
| kvkNummer                | string  | Dutch KVK number. Consists of 8 digits.                                                                                                                                                                                   |
| rsin                     | string  | Legal Entities Partnerships Information Number.                                                                                                                                                                           |
| indNonMailing            | string  | The Company does not wish to receive any unsolicited mail or sales advertising.                                                                                                                                           |
| formeleRegistratiedatum  | string  | The date the branch was registered with the KVK.                                                                                                                                                                          |
| materieleRegistratie     | object  | Starting and end date (if applicable) of the company.                                                                                                                                                                     |
| eersteHandelsnaam        | string  | The name under which a company or branch trades.                                                                                                                                                                          |
| indHoofdvestiging        | string  | Main branch (Yes/No).                                                                                                                                                                                                     |
| indCommercieleVestiging  | string  | Commercial branch (Yes/No).                                                                                                                                                                                               |
| voltijdWerkzamePersonen  | integer | Number of full-time employees.                                                                                                                                                                                            |
| totaalWerkzamePersonen   | integer | Total number of employees.                                                                                                                                                                                                |
| deeltijdWerkzamePersonen | integer | Number of part-time employees.                                                                                                                                                                                            |
| handelsnamen             | array   | All names under which a company or branch trades (in order of registration).                                                                                                                                              |
| adressen                 | array   | List of addresses. See Address table for structure.                                                                                                                                                                       |
| websites                 | array   | Websites registered under main branch.                                                                                                                                                                                    |
| sbiActiviteiten          | array   | Code description of SBI activities in accordance with SBI 2008 (Standard Industrial Classification). No maximum results. See also www.kvk.nl/sbi. Array of items containing sbiCode, sbiOmschrijving, indHoofdactiviteit. |
| links                    | array   | 1. Link to the current query. 2. Link to all branches (based on KVK number). 3. Link to basisprofiel (based on KVK number). 4. Link to vestigingsprofiel (based on branch number).                                        |

### Basisprofiel - eigenaar
### Organisatieprofiel

| Parameter             | Type   | Details                                                                                                                                                                             |
|-----------------------|--------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| rsin                  | string | Legal Entities Partnerships Information Number.                                                                                                                                     |
| rechtsvorm            | string | Legal form of the organisation/company. See Possible output of legal forms.                                                                                                         |
| uitgebreideRechtsvorm | string | Legal form supplemented with information about structure or legal capacity. See Possible output of legal forms.                                                                     |
| adressen              | array  | Are there branches? Then "Adressen" will be displayed at the main branch. Are there no branches? Then "Adressen" will be shown under "Eigenaar". See Address table for structure.   |
| websites              | array  | If there is no main branch, the websites will be shown here.                                                                                                                        |
| links                 | array  | 1. Link to the current query. 2. Link to Basisprofiel (based on KVK-number).                                                                                                        |


### Basisprofiel - vestigingen
### Vestigingsoverzicht

| Parameter                        | Type    | Details                                                                      |
|----------------------------------|---------|------------------------------------------------------------------------------|
| kvkNummer                        | string  | Dutch KVK number. Consists of 8 digits.                                      |
| aantalCommercieleVestigingen     | integer |                                                                              |
| aantalNietCommercieleVestigingen | integer |                                                                              |
| totaalAantalVestigingen          | integer |                                                                              |
| vestigingen                      | array   | The relevant branches. See branch table for structure.                       |
| links                            | array   | 1. Link to the current query. 2. Link to Basisprofiel (based on KVK number). |


### Branch - The data structure of a branch from a basisprofiel vestigingen query.

| Parameter               | Type   | Details                                                                      |
|-------------------------|--------|------------------------------------------------------------------------------|
| vestigingsnummer        | string | Branch number. Unique number consisting of 12 digits.                        |
| eersteHandelsnaam       | string | The name under which a company or establishment trades.                      |
| indHoofdvestiging       | string | Main branch (Yes/No).                                                        |
| indCommercieleVestiging | string | Commercial branch (Yes/No).                                                  |
| volledigAdres           | string | Street, house number, postal code and city.                                  |
| links                   | array  | 1. Link to the current query. 2. Link to Basisprofiel (based on KVK number). |


### Address - The data structure of an address from a Basisprofiel hoofdvestiging or Eigenaar query.

| Parameter            | Type    | Details                                                                                                                                             |
|----------------------|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| type                 | string  | Correspondence and/or visiting address.                                                                                                             |
| IndAfgeschermd       | string  | Address shielded (Yes/No).                                                                                                                          |
| volledigAdres        | string  | Full address                                                                                                                                        |
| straatnaam           | string  | Street name.                                                                                                                                        |
| huisnummer           | string  | House number.                                                                                                                                       |
| huisnummerToevoeging | string  | House number addition. For example: 1 or A.                                                                                                         |
| huisletter           | string  | House letter addition. For example: M.                                                                                                              |
| toevoegingAdres      | string  | Free text to describe an address in detail. For example, when one is located in a multi-company building and there are several floors and/or rooms. |
| postcode             | string  | Postal code.                                                                                                                                        |
| postbusnummer        | integer | Mailbox number.                                                                                                                                     |
| plaats               | string  | Town.                                                                                                                                               |
| straatHuisnummer     | string  | Street name and house number.                                                                                                                       |
| postcodeWoonplaats   | string  | Postal code and Town.                                                                                                                               |
| regio                | string  | Region.                                                                                                                                             |
| land                 | string  | Country.                                                                                                                                            |
| geoData              | object  | BAG ID, GPS coordinates, and national triangulation coordinates.                                                                                    |

### geoData object structure

| Parameter              | Type   | Details                        | Voorbeeld         |
|------------------------|--------|--------------------------------|-------------------|
| addresseerbaarObjectId | string | Unieke BAG ID                  | 0351010000000307  |
| nummerAanduidingId     | string | Unieke BAG nummeraanduiding id | 0351200000000307  |
| gpsLatitude            | double | Lengtegraad                    | 52.0837054156714  |
| gpsLongitude           | double | Breedtegraad                   | 5.423197559040923 |
| rijksdriehoekX         | double | Rijksdriehoek X-coördinaat     | 157467.0          |
| rijksdriehoekY         | double | Rijksdriehoek Y-coördinaat     | 455049.0          |
| rijksdriehoekZ         | double | Rijksdriehoek Z-coördinaat     | 0.0               |


### materieleRegistratie object structure

| Parameter     | Type   | Details                   | Voorbeeld   |
|---------------|--------|---------------------------|-------------|
| datumAanvang  | string | Startdatum onderneming    | "20210917"  |
| datumEinde    | string | Einddatum onderneming     | "20210917"  |


### sbiActiviteiten array[] object Structure

| Parameter          | Type   | Details                                                                               | Voorbeeld                                                                               |
|--------------------|--------|---------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------|
| sbiCode            | string | "62200"                                                                               | "62200"                                                                                 |
| sbiOmschrijving    | string | Activiteiten op het gebied van computerconsultancy en beheer van computerfaciliteiten | "Activiteiten op het gebied van computerconsultancy en beheer van computerfaciliteiten" |
| indHoofdactiviteit | string | Indicates whether this is the main activity (Yes/No)                                  | "Ja"                                                                                    |
