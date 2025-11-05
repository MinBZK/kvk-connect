
Vanuit project-root

$ py -m apps.vestigingen.main --update-known

$ py -m apps.vestigingen.main --update-missing

Docker Images Bouwen en Runnen
==============================
Vanuit project root:
```
 docker build -f apps/vestigingen/Dockerfile -t kvk-vestigingen .
 docker run --rm --env-file .env.docker kvk-vestigingen:latest
```
