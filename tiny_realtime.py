"""
tiny_realtime — Zero-dependency SSE + WebSocket helpers for AI agent streaming.

Provides:
- SSEApp / SSEEmitter — server-sent events router + emitter
- WSApp / WebSocket — WebSocket router + connection wrapper (stdlib WSGI-style)
- stream_sse — decorator to wrap async generators as SSE endpoints
- AIStreamer — buffer for streaming LLM token output
- SSEClient — async context manager that consumes SSE streams

Stdlib only. No external dependencies. Tested on Python 3.8+.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
import uuid
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, List, Optional, Tuple

__version__ = "0.1.0"

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

_SSE_DATA_NEWLINE = re.compile(r"\r?\n")


def _sse_format_field(name: str, value: str) -> str:
    """Format a single SSE field. Multi-line values are split across multiple lines."""
    lines = _SSE_DATA_NEWLINE.split(str(value))
    return "".join(f"{name}: {line}\n" for line in lines)


def _format_sse(event: Optional[str], data: Any, event_id: Optional[str] = None, retry: Optional[int] = None) -> str:
    """Build an SSE frame from fields. Always ends with a blank line."""
    if isinstance(data, (dict, list)):
        data_str = json.dumps(data, separators=(",", ":"), ensure_ascii=False)
    elif data is None:
        data_str = ""
    else:
        data_str = str(data)

    parts: List[str] = []
    if event:
        parts.append(_sse_format_field("event", event))
    if event_id is not None:
        parts.append(_sse_format_field("id", event_id))
    if retry is not None:
        parts.append(f"retry: {int(retry)}\n")
    parts.append(_sse_format_field("data", data_str))
    parts.append("\n")  # blank line terminator
    return "".join(parts)


# ---------------------------------------------------------------------------
# SSEEmitter
# ---------------------------------------------------------------------------


class SSEEmitter:
    """Emits SSE frames to an async response stream. Bound to a `writer` callable."""

    DEFAULT_PING_INTERVAL = 15.0

    def __init__(
        self,
        writer: Callable[[bytes], Awaitable[None]],
        ping_interval: float = DEFAULT_PING_INTERVAL,
        auto_id: bool = False,
    ) -> None:
        self._writer = writer
        self._closed = False
        self._ping_interval = ping_interval
        self._auto_id = auto_id
        self._last_event_id: Optional[int] = None

    @property
    def closed(self) -> bool:
        return self._closed

    async def _send(self, frame: str) -> None:
        if self._closed:
            raise RuntimeError("SSEEmitter is closed")
        await self._writer(frame.encode("utf-8"))

    async def _emit(self, event: Optional[str], data: Any, id: Optional[str]) -> None:
        eid = id or (uuid.uuid4().hex if self._auto_id else None)
        await self._send(_format_sse(event=event, data=data, event_id=eid))

    async def emit(self, event: str, data: Any = "", id: Optional[str] = None) -> None:
        """Send an SSE event. `event` is the event name; `data` is the payload."""
        await self._emit(event, data, id)

    async def emit_data(self, data: Any, id: Optional[str] = None) -> None:
        """Send a default (unnamed) SSE event."""
        await self._emit(None, data, id)

    async def emit_comment(self, comment: str) -> None:
        """Send an SSE comment (lines starting with ':', used for keepalive)."""
        text = str(comment)
        lines = _SSE_DATA_NEWLINE.split(text)
        await self._send("".join(f": {line}\n" for line in lines))

    async def send_raw(self, line: str) -> None:
        """Send a raw SSE-formatted line. Will be encoded as UTF-8."""
        if not line.endswith("\n"):
            line += "\n"
        await self._writer(line.encode("utf-8"))

    async def ping(self) -> None:
        """Send a keepalive comment."""
        await self.emit_comment(f"ping {int(time.time())}")

    async def retry(self, ms: int) -> None:
        """Tell the client to retry after `ms` milliseconds."""
        await self._send(f"retry: {int(ms)}\n\n")

    async def close(self) -> None:
        """Mark the stream as closed. Further emits will raise."""
        self._closed = True


# ---------------------------------------------------------------------------
# SSEApp
# ---------------------------------------------------------------------------


SSEHandler = Callable[[Any, SSEEmitter], Awaitable[None]]


class SSEApp:
    """Routes HTTP requests to SSE handlers. Pairs conceptually with `tiny-router`."""

    DEFAULT_HEADERS = (
        ("Content-Type", "text/event-stream"),
        ("Cache-Control", "no-cache, no-transform"),
        ("Connection", "keep-alive"),
        ("X-Accel-Buffering", "no"),
    )

    def __init__(self, cors_origin: Optional[str] = "*") -> None:
        self._routes: Dict[str, SSEHandler] = {}
        self._cors_origin = cors_origin

    def add_route(self, path: str, handler: SSEHandler) -> None:
        """Register an SSE handler for `path`."""
        if not path.startswith("/"):
            raise ValueError("SSE route path must start with '/'")
        self._routes[path] = handler

    def route(self, path: str) -> Callable[[SSEHandler], SSEHandler]:
        """Decorator form of `add_route`."""

        def decorator(fn: SSEHandler) -> SSEHandler:
            self.add_route(path, fn)
            return fn

        return decorator

    def _build_headers(self) -> List[Tuple[str, str]]:
        headers = list(self.DEFAULT_HEADERS)
        if self._cors_origin is not None:
            headers.append(("Access-Control-Allow-Origin", self._cors_origin))
        return headers

    async def dispatch(self, method: str, path: str, request_headers: Optional[Dict[str, str]] = None) -> Tuple[int, List[Tuple[str, str]], Optional[Callable[[SSEEmitter], Awaitable[None]]]]:
        """Look up a route. Returns (status, headers, run_callable). `run_callable` is None on miss."""
        if method.upper() != "GET":
            return 405, [("Content-Type", "text/plain"), ("Allow", "GET")], None
        handler = self._routes.get(path)
        if handler is None:
            return 404, [("Content-Type", "text/plain")], None

        async def run(emitter: SSEEmitter) -> None:
            await handler(request_headers or {}, emitter)

        return 200, self._build_headers(), run


# ---------------------------------------------------------------------------
# stream_sse decorator
# ---------------------------------------------------------------------------


def stream_sse(event: str = "message"):
    """Decorator: turns an async generator into an SSE handler emitting `event` frames."""

    def decorator(fn: Callable[..., AsyncIterator[Any]]) -> SSEHandler:
        async def handler(req: Dict[str, Any], emitter: SSEEmitter) -> None:
            try:
                async for value in fn(req, emitter):
                    await emitter.emit(event, value)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - defensive
                await emitter.emit("error", {"message": str(exc)})
                await emitter.close()
                return
            await emitter.emit("done", {"ok": True})
            await emitter.close()

        return handler

    return decorator


# ---------------------------------------------------------------------------
# AIStreamer
# ---------------------------------------------------------------------------


class AIStreamer:
    """
    Buffer for AI streaming output. Accumulates text chunks and yields them
    as a stream. Designed for LLM token streaming.

    Usage:
        streamer = AIStreamer()
        await streamer.chunk("Hello")
        await streamer.chunk(" world")
        await streamer.done()

        async for event in streamer.to_async_gen():
            ...
    """

    def __init__(self, event: str = "chunk", include_id: bool = False) -> None:
        self._event = event
        self._queue: "asyncio.Queue[Optional[Dict[str, Any]]]" = asyncio.Queue()
        self._closed = False
        self._id = 0
        self._include_id = include_id
        self._buffer: List[str] = []
        self._done = False
        self._error: Optional[str] = None
        self._start_ts = time.time()
        self._tokens = 0

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def tokens(self) -> int:
        return self._tokens

    @property
    def elapsed(self) -> float:
        return time.time() - self._start_ts

    async def chunk(self, text: str) -> None:
        """Append a text chunk (e.g., one LLM token)."""
        if self._closed:
            raise RuntimeError("AIStreamer is closed")
        self._id += 1
        self._tokens += len(text)
        self._buffer.append(text)
        payload: Dict[str, Any] = {"delta": text, "index": self._id}
        if self._include_id:
            payload["id"] = uuid.uuid4().hex
        await self._queue.put({"event": self._event, "data": payload})

    async def emit(self, event: str, data: Any) -> None:
        """Push a custom event into the stream."""
        if self._closed:
            raise RuntimeError("AIStreamer is closed")
        self._id += 1
        await self._queue.put({"event": event, "data": data})

    async def done(self, meta: Optional[Dict[str, Any]] = None) -> None:
        """Mark the stream complete. Sends a `done` event with metadata."""
        if self._closed:
            return
        body = {
            "ok": True,
            "tokens": self._tokens,
            "elapsed": round(self.elapsed, 4),
            "text": "".join(self._buffer),
        }
        if meta:
            body.update(meta)
        self._done = True
        await self._queue.put({"event": "done", "data": body})
        await self._queue.put(None)
        self._closed = True

    async def error(self, message: str) -> None:
        """Send an error event and close the stream."""
        if self._closed:
            return
        self._error = message
        await self._queue.put({"event": "error", "data": {"message": message}})
        await self._queue.put(None)
        self._closed = True

    async def to_async_gen(self) -> AsyncIterator[Dict[str, Any]]:
        """Yield events as they arrive. `None` is a sentinel for end-of-stream."""
        while True:
            item = await self._queue.get()
            if item is None:
                return
            yield item

    def snapshot(self) -> Dict[str, Any]:
        """Return a snapshot of accumulated state (no waiting)."""
        return {
            "text": "".join(self._buffer),
            "tokens": self._tokens,
            "elapsed": round(self.elapsed, 4),
            "done": self._done,
            "error": self._error,
        }


# ---------------------------------------------------------------------------
# WebSocket (minimal WSGI-style wrapper)
# ---------------------------------------------------------------------------


class WebSocket:
    """
    Minimal WebSocket wrapper. Provides a uniform send/receive API that
    can be backed by any WS server.

    This class does NOT implement the WebSocket framing protocol itself.
    For production WebSocket servers, use `websockets`, `uvicorn`, or
    `hypercorn`. The class is designed so that adapters can wrap their
    server-side sockets and expose the same API to user handlers.
    """

    def __init__(
        self,
        send_text_fn: Callable[[str], Awaitable[None]],
        send_bytes_fn: Callable[[bytes], Awaitable[None]],
        recv_text_fn: Callable[[], Awaitable[str]],
        recv_bytes_fn: Callable[[], Awaitable[bytes]],
        accept_fn: Optional[Callable[[Optional[str]], Awaitable[None]]] = None,
        close_fn: Optional[Callable[[int, str], Awaitable[None]]] = None,
    ) -> None:
        self._send_text = send_text_fn
        self._send_bytes = send_bytes_fn
        self._recv_text = recv_text_fn
        self._recv_bytes = recv_bytes_fn
        self._accept = accept_fn or (lambda subprotocol=None: asyncio.sleep(0))
        self._close = close_fn or (lambda code=1000, reason="": asyncio.sleep(0))
        self._closed = False
        self.subprotocol: Optional[str] = None

    @property
    def closed(self) -> bool:
        return self._closed

    async def accept(self, subprotocol: Optional[str] = None) -> None:
        """Accept the WebSocket upgrade."""
        self.subprotocol = subprotocol
        await self._accept(subprotocol)

    async def send_text(self, text: str) -> None:
        await self._send_text(text)

    async def send_json(self, obj: Any) -> None:
        await self._send_text(json.dumps(obj, separators=(",", ":"), ensure_ascii=False))

    async def send_bytes(self, data: bytes) -> None:
        await self._send_bytes(data)

    async def send_ping(self, data: bytes = b"ping") -> None:
        # Out-of-band control frames: most adapters expose this via a separate
        # channel. The default send_bytes is a placeholder; real adapters
        # override `send_ping` via subclassing or replacement.
        await self._send_bytes(b"PING:" + data)

    async def receive_text(self) -> str:
        return await self._recv_text()

    async def receive_bytes(self) -> bytes:
        return await self._recv_bytes()

    async def receive_json(self) -> Any:
        return json.loads(await self._recv_text())

    async def iter_text(self) -> AsyncIterator[str]:
        while not self._closed:
            try:
                yield await self._recv_text()
            except ConnectionClosed:
                return

    async def iter_json(self) -> AsyncIterator[Any]:
        async for text in self.iter_text():
            yield json.loads(text)

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self._closed = True
        await self._close(code, reason)




class ConnectionClosed(Exception):
    """Raised when a WebSocket connection is closed."""


class WSApp:
    """Routes WebSocket upgrade requests to handlers. Like `SSEApp`, decoupled from a specific server."""

    def __init__(self) -> None:
        self._routes: Dict[str, WSHandler] = {}

    def add_route(self, path: str, handler: "WSHandler") -> None:
        if not path.startswith("/"):
            raise ValueError("WS route path must start with '/'")
        self._routes[path] = handler

    def route(self, path: str) -> Callable[["WSHandler"], "WSHandler"]:
        def decorator(fn: "WSHandler") -> "WSHandler":
            self.add_route(path, fn)
            return fn

        return decorator

    def match(self, path: str) -> Optional["WSHandler"]:
        return self._routes.get(path)


WSHandler = Callable[[WebSocket, Dict[str, Any]], Awaitable[None]]


# ---------------------------------------------------------------------------
# SSEClient (basic async consumer)
# ---------------------------------------------------------------------------


_SSE_LINE_RE = re.compile(r"^(?P<field>[^:\r\n]+)(?::(?P<value>.*))?$")


async def _default_stream(url: str, headers: Optional[Dict[str, str]]):
    """
    Default SSE stream fetcher. Uses urllib via asyncio's run_in_executor
    to avoid any external deps. Returns an async iterator of raw bytes.
    For production use, override with httpx/aiohttp-based streaming.
    """
    import urllib.request

    req = urllib.request.Request(url, headers=headers or {})
    loop = asyncio.get_event_loop()

    def _open():
        return urllib.request.urlopen(req, timeout=30)

    response = await loop.run_in_executor(None, _open)
    try:
        while True:
            chunk = await loop.run_in_executor(None, response.read, 4096)
            if not chunk:
                return
            yield chunk
    finally:
        response.close()


class SSEClient:
    """
    Async SSE client. Parses SSE frames from a stream.

    Example:
        async with SSEClient("https://example.com/stream") as client:
            async for event_type, data in client.events():
                print(event_type, data)
    """

    def __init__(self, url: str, headers: Optional[Dict[str, str]] = None, stream: Optional[Callable[[str, Optional[Dict[str, str]]], AsyncIterator[bytes]]] = None) -> None:
        self.url = url
        self.headers = headers or {}
        self._stream_factory = stream or _default_stream
        self._closed = False

    async def __aenter__(self) -> "SSEClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def events(self) -> AsyncIterator[Tuple[Optional[str], str]]:
        """Async iterator yielding (event_type, data) tuples. `event_type` is None for unnamed events."""
        buf = ""
        event: Optional[str] = None
        data_lines: List[str] = []
        event_id: Optional[str] = None

        async for chunk in self._stream_factory(self.url, self.headers):
            if self._closed:
                return
            buf += chunk.decode("utf-8", errors="replace")
            while "\n" in buf:
                line, buf = buf.split("\n", 1)
                line = line.rstrip("\r")
                if line == "":
                    if data_lines or event is not None or event_id is not None:
                        yield event, "\n".join(data_lines)
                    event = None
                    data_lines = []
                    event_id = None
                    continue
                if line.startswith(":"):
                    continue  # comment
                m = _SSE_LINE_RE.match(line)
                if not m:
                    continue
                field = m.group("field").strip()
                value = (m.group("value") or "").lstrip(" ")
                if field == "event":
                    event = value
                elif field == "data":
                    data_lines.append(value)
                elif field == "id":
                    event_id = value
                elif field == "retry":
                    # not surfaced as event, honored on reconnect only
                    continue

    async def close(self) -> None:
        self._closed = True


# ---------------------------------------------------------------------------
# Convenience helpers
# ---------------------------------------------------------------------------


def make_sse_response_headers(extra: Optional[Dict[str, str]] = None) -> List[Tuple[str, str]]:
    """Build a default SSE response header list."""
    headers = list(SSEApp.DEFAULT_HEADERS)
    if extra:
        headers.extend(extra.items())
    return headers


async def iter_to_sse(stream: AsyncIterator[Any], emitter: SSEEmitter, event: str = "message") -> None:
    """Pump an async iterator into an SSE emitter."""
    async for item in stream:
        await emitter.emit(event, item)


__all__ = [
    "__version__",
    "SSEApp",
    "SSEEmitter",
    "SSEClient",
    "stream_sse",
    "AIStreamer",
    "WebSocket",
    "WSApp",
    "ConnectionClosed",
    "make_sse_response_headers",
    "iter_to_sse",
]
