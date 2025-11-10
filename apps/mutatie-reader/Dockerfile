FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Europe/Amsterdam

# Install ODBC dependencies for MSSQL support
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    unixodbc \
    unixodbc-dev \
    apt-transport-https \
    ca-certificates \
    && curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /etc/apt/trusted.gpg.d/microsoft.gpg \
    && echo "deb [arch=amd64] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency files FIRST
COPY pyproject.toml uv.lock README.md ./
COPY src ./src

# Install only production dependencies
RUN uv sync --frozen --no-dev

# Copy source code
COPY apps/mutatie-reader ./apps/mutatie-reader

ENV PATH=/app/.venv/bin:$PATH \
    PYTHONPATH=/app/src

# Use virtual environment Python to run the mutatie-reader
CMD ["uv", "run", "--no-sync", "python", "apps/mutatie-reader/main.py", "--auto", "--daemon", "--interval", "60"]
