![Tests](https://github.com/MinBZK/kvk-connect/actions/workflows/ci.yml/badge.svg)
[![PyPI](https://img.shields.io/pypi/v/kvk-connect)](https://pypi.org/project/kvk-connect/)

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

De vijf Docker apps werken onafhankelijk van elkaar samen om de KVK data actueel te houden:

![AppsStructure](docs/apps.drawio.svg)


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
