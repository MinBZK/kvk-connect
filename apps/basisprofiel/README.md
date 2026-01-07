
Vanuit project-root

# Update missing records in daemon mode
$ python apps/basisprofiel/main.py --update-missing

# Update known outdated records in daemon mode
$ python apps/basisprofiel/main.py --update-known

# Update single kvk record
$ uv run python -m apps.basisprofiel.main --kvk 56850042


Docker Images Bouwen en Runnen
==============================
Vanuit project root:
```
 docker build -f apps/basisprofiel/Dockerfile -t kvk-basisprofiel .
 docker run --rm --env-file .env.docker kvk-basisprofiel:latest
```
