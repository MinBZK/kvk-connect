"""Regressietests voor de nginx gateway configuratie template.

Valideert dat de geredenderde nginx.conf.template alle directives bevat
die nodig zijn voor correcte TLS/HTTP communicatie met de KVK API, en
geen directives bevat die aantoonbaar breken (v1.7.3/v1.7.4/v1.7.5).
"""

from __future__ import annotations

from pathlib import Path

TEMPLATE_PATH = Path(__file__).resolve().parents[3] / "apps" / "gateway" / "nginx.conf.template"


def _render(*, api_host: str = "api.kvk.nl", rate_limit: str = "100") -> str:
    """Emuleer envsubst met de whitelist uit apps/gateway/entrypoint.sh."""
    return (
        TEMPLATE_PATH.read_text(encoding="utf-8")
        .replace("${KVK_API_HOST}", api_host)
        .replace("${RATE_LIMIT_CALLS}", rate_limit)
    )


def test_proxy_pass_uses_literal_hostname() -> None:
    """proxy_pass moet een literal hostname hebben (geen $-variabele).

    Variabele-vorm (proxy_pass https://$var/...) laat nginx de upstream-SSL-connectie
    op een manier opzetten die KVK's load balancer afwijst op native Linux
    nginx:1.25-alpine — alle requests krijgen dan 502 'upstream prematurely closed'
    ~60ms na de HTTP-request (v1.7.5-regressie).
    """
    assert "proxy_pass https://api.kvk.nl/api/v1/" in _render()


def test_proxy_ssl_name_not_used() -> None:
    """proxy_ssl_name hoort niet in de config: met literal proxy_pass leidt nginx
    SNI automatisch correct af van de hostname. Expliciet zetten was een
    doodlopend spoor uit v1.7.4."""
    assert "proxy_ssl_name" not in _render()


def test_no_nginx_variable_in_proxy_pass() -> None:
    """Belangrijkste guard: geen $-variabele in de actieve proxy_pass directive.
    Zorgt dat een toekomstige 'kleine optimalisatie' de v1.7.3-regressie
    (runtime-resolved hostname breekt TLS richting KVK) niet opnieuw introduceert."""
    directive_lines = [
        line.strip()
        for line in _render().splitlines()
        if line.strip().startswith("proxy_pass ")
    ]
    assert directive_lines, "Geen proxy_pass directive gevonden"
    for line in directive_lines:
        assert "$" not in line, f"proxy_pass mag geen nginx-variabele bevatten: {line}"


def test_ssl_server_name_enabled() -> None:
    assert "proxy_ssl_server_name on" in _render()


def test_host_header_matches_upstream_literal() -> None:
    """Host-header moet literal zijn (1-op-1 met de hostname in proxy_pass)."""
    assert "proxy_set_header Host api.kvk.nl" in _render()


def test_api_host_is_substituted_from_env() -> None:
    rendered = _render(api_host="test.example.com")
    assert "proxy_pass https://test.example.com/api/v1/" in rendered
    assert "proxy_set_header Host test.example.com" in rendered


def test_rate_limit_is_substituted_from_env() -> None:
    assert "rate=50r/s" in _render(rate_limit="50")


def test_envsubst_placeholders_fully_resolved() -> None:
    """Na envsubst mogen er geen ${...} placeholders meer staan in de gerenderde config."""
    rendered = _render()
    assert "${KVK_API_HOST}" not in rendered
    assert "${RATE_LIMIT_CALLS}" not in rendered
