"""MCP client: connects to MCP servers and wraps their tools as native nanobot tools."""

import asyncio
from contextlib import AsyncExitStack
from typing import Any

import httpx
from loguru import logger

from nanobot.agent.tools.base import Tool
from nanobot.agent.tools.registry import ToolRegistry


class MCPServerRuntime:
    """Own a single MCP server connection and reconnect it on demand."""

    def __init__(self, server_name: str, cfg):
        self.server_name = server_name
        self.cfg = cfg
        self._session = None
        self._stack: AsyncExitStack | None = None
        self._connect_lock = asyncio.Lock()

    async def __aenter__(self) -> "MCPServerRuntime":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        await self.close()
        return False

    @property
    def session(self):
        return self._session

    async def connect(self) -> None:
        """Open the underlying MCP transport and initialize the session."""
        from mcp import ClientSession, StdioServerParameters
        from mcp.client.sse import sse_client
        from mcp.client.stdio import stdio_client
        from mcp.client.streamable_http import streamable_http_client

        stack = AsyncExitStack()
        await stack.__aenter__()

        try:
            transport_type = self.cfg.type
            if not transport_type:
                if self.cfg.command:
                    transport_type = "stdio"
                elif self.cfg.url:
                    transport_type = (
                        "sse" if self.cfg.url.rstrip("/").endswith("/sse") else "streamableHttp"
                    )
                else:
                    raise ValueError("no command or url configured")

            if transport_type == "stdio":
                params = StdioServerParameters(
                    command=self.cfg.command,
                    args=self.cfg.args,
                    env=self.cfg.env or None,
                )
                read, write = await stack.enter_async_context(stdio_client(params))
            elif transport_type == "sse":
                def httpx_client_factory(
                    headers: dict[str, str] | None = None,
                    timeout: httpx.Timeout | None = None,
                    auth: httpx.Auth | None = None,
                ) -> httpx.AsyncClient:
                    merged_headers = {**(self.cfg.headers or {}), **(headers or {})}
                    return httpx.AsyncClient(
                        headers=merged_headers or None,
                        follow_redirects=True,
                        timeout=timeout,
                        auth=auth,
                    )

                read, write = await stack.enter_async_context(
                    sse_client(self.cfg.url, httpx_client_factory=httpx_client_factory)
                )
            elif transport_type == "streamableHttp":
                http_client = await stack.enter_async_context(
                    httpx.AsyncClient(
                        headers=self.cfg.headers or None,
                        follow_redirects=True,
                        timeout=None,
                    )
                )
                read, write, _ = await stack.enter_async_context(
                    streamable_http_client(self.cfg.url, http_client=http_client)
                )
            else:
                raise ValueError(f"unknown transport type '{transport_type}'")

            session = await stack.enter_async_context(ClientSession(read, write))
            await session.initialize()
        except Exception:
            await stack.aclose()
            raise

        self._stack = stack
        self._session = session

    async def close(self) -> None:
        """Close the active MCP transport, if any."""
        stack, self._stack = self._stack, None
        self._session = None
        if stack:
            try:
                await stack.aclose()
            except Exception:
                pass

    async def reconnect(self) -> bool:
        """Reconnect the server after a transport/session failure."""
        async with self._connect_lock:
            await self.close()
            try:
                await self.connect()
            except Exception as exc:
                logger.error("MCP server '{}': reconnect failed: {}", self.server_name, exc)
                return False
            logger.info("MCP server '{}': reconnected", self.server_name)
            return True

    async def list_tools(self):
        if self._session is None:
            raise RuntimeError(f"MCP server '{self.server_name}' is not connected")
        return await self._session.list_tools()

    async def call_tool(self, tool_name: str, arguments: dict[str, Any]):
        if self._session is None:
            raise RuntimeError(f"MCP server '{self.server_name}' is not connected")
        return await self._session.call_tool(tool_name, arguments=arguments)


class MCPToolWrapper(Tool):
    """Wraps a single MCP server tool as a nanobot Tool."""

    def __init__(self, runtime: MCPServerRuntime, server_name: str, tool_def, tool_timeout: int = 30):
        self._runtime = runtime
        self._original_name = tool_def.name
        self._name = f"mcp_{server_name}_{tool_def.name}"
        self._description = tool_def.description or tool_def.name
        self._parameters = tool_def.inputSchema or {"type": "object", "properties": {}}
        self._tool_timeout = tool_timeout

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return self._description

    @property
    def parameters(self) -> dict[str, Any]:
        return self._parameters

    async def execute(self, **kwargs: Any) -> str:
        from mcp import types

        result = None

        def _is_connection_failure(exc: Exception) -> bool:
            text = str(exc).lower()
            return any(
                token in text
                for token in (
                    "closed",
                    "disconnect",
                    "broken pipe",
                    "connection reset",
                    "end of stream",
                    "not connected",
                    "session",
                    "transport",
                )
            )

        try:
            result = await asyncio.wait_for(
                self._runtime.call_tool(self._original_name, arguments=kwargs),
                timeout=self._tool_timeout,
            )
        except asyncio.TimeoutError:
            logger.warning("MCP tool '{}' timed out after {}s", self._name, self._tool_timeout)
            return f"(MCP tool call timed out after {self._tool_timeout}s)"
        except asyncio.CancelledError:
            # MCP SDK's anyio cancel scopes can leak CancelledError on timeout/failure.
            # Re-raise only if our task was externally cancelled (e.g. /stop).
            task = asyncio.current_task()
            if task is not None and task.cancelling() > 0:
                raise
            logger.warning("MCP tool '{}' was cancelled by server/SDK", self._name)
            return "(MCP tool call was cancelled)"
        except Exception as exc:
            if _is_connection_failure(exc):
                logger.warning(
                    "MCP tool '{}' failed due to connection loss ({}); attempting reconnect",
                    self._name,
                    type(exc).__name__,
                )
                if await self._runtime.reconnect():
                    try:
                        result = await asyncio.wait_for(
                            self._runtime.call_tool(self._original_name, arguments=kwargs),
                            timeout=self._tool_timeout,
                        )
                    except asyncio.TimeoutError:
                        logger.warning(
                            "MCP tool '{}' timed out after reconnect ({}s)",
                            self._name,
                            self._tool_timeout,
                        )
                        return f"(MCP tool call timed out after {self._tool_timeout}s)"
                    except asyncio.CancelledError:
                        task = asyncio.current_task()
                        if task is not None and task.cancelling() > 0:
                            raise
                        logger.warning("MCP tool '{}' was cancelled after reconnect", self._name)
                        return "(MCP tool call was cancelled)"
                    except Exception as retry_exc:
                        exc = retry_exc
                    else:
                        exc = None
                else:
                    exc = exc
            if exc is None:
                parts = []
                for block in result.content:
                    if isinstance(block, types.TextContent):
                        parts.append(block.text)
                    else:
                        parts.append(str(block))
                return "\n".join(parts) or "(no output)"
            logger.exception(
                "MCP tool '{}' failed: {}: {}",
                self._name,
                type(exc).__name__,
                exc,
            )
            return f"(MCP tool call failed: {type(exc).__name__})"

        parts = []
        for block in result.content:
            if isinstance(block, types.TextContent):
                parts.append(block.text)
            else:
                parts.append(str(block))
        return "\n".join(parts) or "(no output)"


async def connect_mcp_servers(
    mcp_servers: dict, registry: ToolRegistry, stack: AsyncExitStack
) -> None:
    """Connect to configured MCP servers and register their tools."""
    for name, cfg in mcp_servers.items():
        try:
            if not cfg.command and not cfg.url:
                logger.warning("MCP server '{}': no command or url configured, skipping", name)
                continue

            runtime = await stack.enter_async_context(MCPServerRuntime(name, cfg))
            tools = await runtime.list_tools()
            enabled_tools = set(cfg.enabled_tools)
            allow_all_tools = "*" in enabled_tools
            registered_count = 0
            matched_enabled_tools: set[str] = set()
            available_raw_names = [tool_def.name for tool_def in tools.tools]
            available_wrapped_names = [f"mcp_{name}_{tool_def.name}" for tool_def in tools.tools]
            for tool_def in tools.tools:
                wrapped_name = f"mcp_{name}_{tool_def.name}"
                if (
                    not allow_all_tools
                    and tool_def.name not in enabled_tools
                    and wrapped_name not in enabled_tools
                ):
                    logger.debug(
                        "MCP: skipping tool '{}' from server '{}' (not in enabledTools)",
                        wrapped_name,
                        name,
                    )
                    continue
                wrapper = MCPToolWrapper(runtime, name, tool_def, tool_timeout=cfg.tool_timeout)
                registry.register(wrapper)
                logger.debug("MCP: registered tool '{}' from server '{}'", wrapper.name, name)
                registered_count += 1
                if enabled_tools:
                    if tool_def.name in enabled_tools:
                        matched_enabled_tools.add(tool_def.name)
                    if wrapped_name in enabled_tools:
                        matched_enabled_tools.add(wrapped_name)

            if enabled_tools and not allow_all_tools:
                unmatched_enabled_tools = sorted(enabled_tools - matched_enabled_tools)
                if unmatched_enabled_tools:
                    logger.warning(
                        "MCP server '{}': enabledTools entries not found: {}. Available raw names: {}. "
                        "Available wrapped names: {}",
                        name,
                        ", ".join(unmatched_enabled_tools),
                        ", ".join(available_raw_names) or "(none)",
                        ", ".join(available_wrapped_names) or "(none)",
                    )

            logger.info("MCP server '{}': connected, {} tools registered", name, registered_count)
        except Exception as e:
            logger.error("MCP server '{}': failed to connect: {}", name, e)
