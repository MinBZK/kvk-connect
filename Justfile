# Alias
alias t := test
alias a := check-all

# Set the default shell for all platforms
set shell := ["sh", "-cu"]
set windows-shell := ["cmd.exe", "/C"]

set dotenv-load

# Set the default recipe
@_:
    just --list


# Run tests
[group('qa')]
test *args:
    uv run -m pytest {{ args }}

_cov *args:
    uv run -m coverage {{ args }}

# Run tests and measure coverage
[group('qa')]
@cov:
    just _cov erase
    just _cov run -m pytest tests
    just _cov report
    just _cov html

# Run linters
[group('qa')]
lint:
    uvx ruff check
    uvx ruff format

# Check types
[group('qa')]
typing:
    uv run pyright

# Check pre-commit hooks
[group('qa')]
pc:
    uv run pre-commit run --all-files

# Perform all checks
[group('qa')]
check-all: lint cov typing pc


# Update dependencies
[group('lifecycle')]
update:
    uv sync --upgrade

# Ensure project virtualenv is up to date
[group('lifecycle')]
install:
    uv sync

[group('deployment')]
_clean_dist:
    just _clean-dist-{{os()}}

_clean-dist-linux:
    rm -rf dist
_clean-dist-macos:
    rm -rf .dist
    find . -type d -name "__pycache__" -exec rm -r {} +
_clean-dist-windows:
    -rmdir /s /q dist

# build the distribution packages
[group('deployment')]
build:
    just _clean_dist
    uv build

# create and push a git tag use: tag name and message
[group('deployment')]
tag tag msg:
    git tag -a {{tag}} -m "{{msg}}"
    git push origin {{tag}}

# bump the version in pyproject.toml use: patch, minor, or major
[group('deployment')]
bump tag:
    uv version --bump {{tag}}

# publish the package to PyPI
[group('deployment')]
deploy version:
    just build
    uv publish --index testpypi
    @echo 'Package v{{ version }} published successfully'


# Docker compose up (local by default, use 'env=prod' for production)
[group('docker')]
docker-build env='local':
    docker compose -f docker-compose.{{env}}.yaml build

[group('docker')]
docker-up env='local':
    docker compose -f docker-compose.{{env}}.yaml up -d

# Docker compose down
[group('docker')]
docker-down env='local':
    docker compose -f docker-compose.{{env}}.yaml down

# View logs from Docker services
[group('docker')]
docker-logs env='local' *service:
    docker compose -f docker-compose.{{env}}.yaml logs -f {{ service }}

# Restart Docker services
[group('docker')]
docker-restart env='local':
    docker compose -f docker-compose.{{env}}.yaml restart
