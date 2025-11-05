
Vanuit project-root

# Update missing records in daemon mode
$ python apps/vestigingsprofiel/main.py --update-missing

# Update known outdated records in daemon mode
$ python apps/vestigingsprofiel/main.py --update-known

Docker Images Bouwen en Runnen
==============================
Vanuit project root:
```
 docker build -f apps/vestigingsprofiel/Dockerfile -t kvk-vestigingsprofiel .
 docker run --rm --env-file .env.docker kvk-vestigingsprofiel:latest
```
