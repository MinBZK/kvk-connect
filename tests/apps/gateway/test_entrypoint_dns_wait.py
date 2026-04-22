"""Regressietests voor apps/gateway/entrypoint.sh (v1.7.5).

De entrypoint moet wachten tot DNS de upstream-hostname kan resolven voordat
nginx gestart wordt. Zonder deze wait komt de v1.7.2-startup-crash terug op
native Linux hosts waar de container-DNS niet direct beschikbaar is.
"""

from __future__ import annotations

from pathlib import Path

ENTRYPOINT_PATH = Path(__file__).resolve().parents[3] / "apps" / "gateway" / "entrypoint.sh"


def _content() -> str:
    return ENTRYPOINT_PATH.read_text(encoding="utf-8")


def test_entrypoint_waits_for_dns_on_upstream() -> None:
    """getent hosts moet aangeroepen worden op ${KVK_API_HOST} vóór nginx start."""
    content = _content()
    assert 'getent hosts "${KVK_API_HOST}"' in content


def test_dns_wait_precedes_nginx_start() -> None:
    """DNS-wait moet vóór de envsubst+nginx-exec komen, anders heeft het geen zin."""
    content = _content()
    dns_idx = content.find("getent hosts")
    nginx_idx = content.find("exec nginx")
    assert dns_idx != -1 and nginx_idx != -1
    assert dns_idx < nginx_idx


def test_entrypoint_fails_fast_if_dns_never_resolves() -> None:
    """Geen oneindige wait-loop — bij aanhoudende DNS-fail moet de entrypoint exit 1
    geven zodat Docker de container als unhealthy ziet en Watchtower kan ingrijpen."""
    assert "exit 1" in _content()
