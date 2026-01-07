
Vanuit project-root

$ py -m apps.mutatie-reader.main CMD --auto

Docker Images Bouwen en Runnen
==============================
Vanuit project root:
```
 docker build -f apps/mutatie-reader/Dockerfile -t kvk-mutatie-reader .
 docker run --rm --env-file .env.docker kvk-mutatie-reader:latest
```


# Ophalen van een single signaalid
```
 uv run python -m apps.mutatie-reader.main --signaalid a131db31-c567-3894-a96a-84dc8d96a5f5
 
```
