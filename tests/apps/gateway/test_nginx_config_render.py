"""Regressietests voor de nginx gateway configuratie template.

Valideert dat de geredenderde nginx.conf.template alle directives bevat
die nodig zijn voor correcte TLS/HTTP communicatie met de KVK API.
Voorkomt regressies zoals v1.7.3 (SNI kwijt bij variabele-proxy_pass).
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


def test_proxy_pass_uses_variable_for_runtime_dns() -> None:
    """proxy_pass moet een nginx-variabele bevatten, anders resolvet nginx bij startup (v1.7.2 crash)."""
    assert "proxy_pass https://$kvk_upstream/api/v1/" in _render()


def test_explicit_sni_hostname_is_configured() -> None:
    """proxy_ssl_name moet expliciet gezet zijn — bij variabele-proxy_pass stuurt nginx anders geen SNI (v1.7.3 502)."""
    assert "proxy_ssl_name $kvk_upstream" in _render()


def test_ssl_server_name_enabled() -> None:
    assert "proxy_ssl_server_name on" in _render()


def test_http_version_1_1_to_upstream() -> None:
    """KVK accepteert HTTP/1.0 niet betrouwbaar — gateway moet 1.1 doorsturen."""
    assert "proxy_http_version 1.1" in _render()


def test_connection_header_cleared_for_keepalive() -> None:
    """Lege Connection header is vereist voor HTTP/1.1 keep-alive naar upstream."""
    assert 'proxy_set_header Connection ""' in _render()


def test_host_header_matches_upstream() -> None:
    assert "proxy_set_header Host $kvk_upstream" in _render()


def test_docker_internal_resolver_configured() -> None:
    assert "resolver 127.0.0.11" in _render()


def test_upstream_variable_is_substituted_from_env() -> None:
    rendered = _render(api_host="test.example.com")
    assert "set $kvk_upstream test.example.com" in rendered


def test_rate_limit_is_substituted_from_env() -> None:
    assert "rate=50r/s" in _render(rate_limit="50")


def test_envsubst_placeholders_fully_resolved() -> None:
    """Na envsubst mogen er geen ${...} placeholders meer staan in de gerenderde config."""
    rendered = _render()
    assert "${KVK_API_HOST}" not in rendered
    assert "${RATE_LIMIT_CALLS}" not in rendered
