"""Tests voor de @log_tool_call decorator in de MCP-server app."""

from __future__ import annotations

import asyncio
import logging

import pytest

# Import de decorator direct vanuit de app module
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "apps", "mcp-server"))


class TestLogToolCallDecorator:
    def test_resultaat_doorgegeven(self) -> None:
        from main import log_tool_call  # type: ignore[import]

        @log_tool_call
        async def _tool(x: str) -> str:
            return f"ok:{x}"

        result = asyncio.run(_tool("test"))
        assert result == "ok:test"

    def test_logt_ok_bij_succes(self, caplog: pytest.LogCaptureFixture) -> None:
        from main import log_tool_call  # type: ignore[import]

        @log_tool_call
        async def _mijn_tool(x: str) -> str:
            return "klaar"

        with caplog.at_level(logging.INFO):
            asyncio.run(_mijn_tool("invoer"))

        assert any("_mijn_tool" in r.message and "OK" in r.message for r in caplog.records)

    def test_logt_error_en_propageert_exception(self, caplog: pytest.LogCaptureFixture) -> None:
        from main import log_tool_call  # type: ignore[import]

        @log_tool_call
        async def _kapotte_tool(x: str) -> str:
            raise ValueError("iets ging fout")

        with caplog.at_level(logging.ERROR):
            with pytest.raises(ValueError, match="iets ging fout"):
                asyncio.run(_kapotte_tool("test"))

        assert any("_kapotte_tool" in r.message and "ERROR" in r.message for r in caplog.records)

    def test_timing_in_logbericht(self, caplog: pytest.LogCaptureFixture) -> None:
        from main import log_tool_call  # type: ignore[import]

        @log_tool_call
        async def _tool(x: str) -> str:
            return "snel"

        with caplog.at_level(logging.INFO):
            asyncio.run(_tool("x"))

        assert any("ms" in r.message for r in caplog.records)
