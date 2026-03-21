"""NapCat channel implementation using OneBot 11 forward WebSocket."""

from __future__ import annotations

import asyncio
import json
from collections import OrderedDict
from itertools import count
from typing import Any, Literal

from loguru import logger
from pydantic import Field
import websockets

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.schema import Base


class NapCatConfig(Base):
    """NapCat OneBot WebSocket channel configuration."""

    enabled: bool = False
    url: str = "ws://127.0.0.1:3001/"
    access_token: str = ""
    allow_from: list[str] = Field(default_factory=list)
    group_policy: Literal["open", "mention"] = "mention"
    reconnect_delay_s: float = 5.0


class NapCatChannel(BaseChannel):
    """NapCat channel using OneBot 11 over forward WebSocket."""

    name = "napcat"
    display_name = "NapCat"

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        return NapCatConfig().model_dump(by_alias=True)

    def __init__(self, config: Any, bus: MessageBus):
        if isinstance(config, dict):
            config = NapCatConfig.model_validate(config)
        super().__init__(config, bus)
        self.config: NapCatConfig = config
        self._ws: Any = None
        self._self_id: str | None = None
        self._chat_type_cache: dict[str, str] = {}
        self._processed_ids: OrderedDict[str, None] = OrderedDict()
        self._echo_counter = count(1)
        self._send_lock = asyncio.Lock()

    async def start(self) -> None:
        """Start the NapCat forward WebSocket client."""
        self._running = True
        while self._running:
            try:
                await self._run_connection()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("NapCat websocket error: {}", e)
            finally:
                self._ws = None

            if self._running:
                logger.info("Reconnecting NapCat in {}s...", self.config.reconnect_delay_s)
                await asyncio.sleep(self.config.reconnect_delay_s)

    async def stop(self) -> None:
        """Stop the NapCat channel."""
        self._running = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

    async def send(self, msg: OutboundMessage) -> None:
        """Send a plain-text message via OneBot action requests."""
        if not self._ws:
            logger.warning("NapCat websocket not connected")
            return

        is_group = bool(
            msg.metadata.get("is_group")
            if msg.metadata
            else self._chat_type_cache.get(msg.chat_id) == "group"
        )
        action = "send_group_msg" if is_group else "send_private_msg"
        target_key = "group_id" if is_group else "user_id"
        payload = {
            "action": action,
            "params": {target_key: msg.chat_id, "message": msg.content},
            "echo": f"nanobot:{next(self._echo_counter)}",
        }

        async with self._send_lock:
            await self._ws.send(json.dumps(payload, ensure_ascii=False))

    async def _run_connection(self) -> None:
        """Open the websocket and process inbound events until disconnect."""
        headers = self._connect_headers()
        async with websockets.connect(self.config.url, additional_headers=headers or None) as ws:
            self._ws = ws
            logger.info("NapCat websocket connected to {}", self.config.url)
            async for raw in ws:
                await self._handle_ws_message(raw)

    def _connect_headers(self) -> dict[str, str]:
        """Build websocket auth headers."""
        if not self.config.access_token:
            return {}
        return {"Authorization": f"Bearer {self.config.access_token}"}

    async def _handle_ws_message(self, raw: str) -> None:
        """Handle a raw websocket frame from NapCat."""
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Invalid NapCat JSON: {}", raw[:200])
            return

        if not isinstance(data, dict):
            return

        if "self_id" in data and data["self_id"] is not None:
            self._self_id = str(data["self_id"])

        if "echo" in data and "status" in data:
            if data.get("status") != "ok":
                logger.warning(
                    "NapCat action {} failed: status={} retcode={} message={}",
                    data.get("echo"), data.get("status"), data.get("retcode"), data.get("message"),
                )
            return

        if data.get("post_type") != "message":
            return

        await self._handle_event(data)

    async def _handle_event(self, event: dict[str, Any]) -> None:
        """Handle an inbound OneBot message event."""
        message_id = str(event.get("message_id") or "")
        if message_id:
            if message_id in self._processed_ids:
                return
            self._processed_ids[message_id] = None
            while len(self._processed_ids) > 1000:
                self._processed_ids.popitem(last=False)

        user_id = str(event.get("user_id") or "")
        if not user_id:
            return

        if self._self_id and user_id == self._self_id:
            return

        message_type = str(event.get("message_type") or "")
        if message_type == "group":
            chat_id = str(event.get("group_id") or "")
            self._chat_type_cache[chat_id] = "group"
        elif message_type == "private":
            chat_id = user_id
            self._chat_type_cache[chat_id] = "private"
        else:
            return

        segments = event.get("message")
        text = self._segments_to_text(segments)
        media = self._segments_to_media(segments)

        if message_type == "group" and self.config.group_policy == "mention":
            if not self._contains_self_mention(segments):
                return

        if not text and not media:
            return

        await self._handle_message(
            sender_id=user_id,
            chat_id=chat_id,
            content=text,
            media=media,
            metadata={"message_id": message_id, "is_group": message_type == "group"},
        )

    def _contains_self_mention(self, segments: Any) -> bool:
        """Check whether the message explicitly @mentions this bot."""
        if not isinstance(segments, list) or not self._self_id:
            return False
        for segment in segments:
            if not isinstance(segment, dict) or segment.get("type") != "at":
                continue
            qq = str((segment.get("data") or {}).get("qq") or "")
            if qq == self._self_id:
                return True
        return False

    @staticmethod
    def _segments_to_text(segments: Any) -> str:
        """Extract visible text from OneBot segments."""
        if isinstance(segments, str):
            return segments.strip()
        if not isinstance(segments, list):
            return ""

        parts: list[str] = []
        for segment in segments:
            if not isinstance(segment, dict):
                continue
            data = segment.get("data") or {}
            if segment.get("type") == "text":
                text = str(data.get("text") or "")
                if text:
                    parts.append(text)
        return "".join(parts).strip()

    @staticmethod
    def _segments_to_media(segments: Any) -> list[str]:
        """Extract image URLs from OneBot message segments."""
        if not isinstance(segments, list):
            return []
        media: list[str] = []
        for segment in segments:
            if not isinstance(segment, dict) or segment.get("type") != "image":
                continue
            url = str((segment.get("data") or {}).get("url") or "").strip()
            if url:
                media.append(url)
        return media
