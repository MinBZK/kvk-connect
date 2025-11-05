
Vanuit project-root

$ py -m apps.mutatie-reader.main CMD --auto

Docker Images Bouwen en Runnen
==============================
Vanuit project root:
```
 docker build -f apps/mutatie-reader/Dockerfile -t kvk-mutatie-reader .
 docker run --rm --env-file .env.docker kvk-mutatie-reader:latest
```


### Ophalen van een single signaalid

```
 python -m apps.mutatie-reader.main --signaalid 604afefc-b8c1-3270-85f9-486a2ec807eb
```
