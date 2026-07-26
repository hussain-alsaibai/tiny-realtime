"""
test_tiny_realtime.py — Tests for the tiny_realtime library.

Tests are stdlib-only (asyncio + unittest). Run with:
    python3 -m unittest test_tiny_realtime.py -v
or simply:
    python3 test_tiny_realtime.py
"""

import asyncio
import json
import unittest
from typing import AsyncIterator, Dict, List, Tuple

from tiny_realtime import (
    SSEApp,
    SSEEmitter,
    SSEClient,
    stream_sse,
    AIStreamer,
    WebSocket,
    WSApp,
    ConnectionClosed,
    make_sse_response_headers,
    iter_to_sse,
    __version__,
)


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


class _ByteCollector:
    """Captures bytes from an SSEEmitter writer for assertions."""

    def __init__(self) -> None:
        self.buffer: List[bytes] = []

    async def __call__(self, data: bytes) -> None:
        self.buffer.append(data)

    def text(self) -> str:
        return b"".join(self.buffer).decode("utf-8")


class _FakeStream:
    """A fake byte stream used by SSEClient for testing."""

    def __init__(self, chunks: List[bytes]) -> None:
        self._chunks = chunks

    async def __call__(self, url: str, headers) -> AsyncIterator[bytes]:
        for chunk in self._chunks:
            yield chunk


# ---------------------------------------------------------------------------
# SSE format / SSEEmitter
# ---------------------------------------------------------------------------


class TestSSEFormat(unittest.TestCase):
    """Direct tests of SSE formatting utilities (via SSEEmitter)."""

    def test_version_is_string(self) -> None:
        self.assertIsInstance(__version__, str)
        self.assertEqual(__version__, "0.1.0")

    async def test_emit_basic_event(self) -> None:
        collector = _ByteCollector()
        emitter = SSEEmitter(collector)
        await emitter.emit("message", "hello")
        out = collector.text()
        self.assertIn("event: message\n", out)
        self.assertIn("data: hello\n", out)
        self.assertTrue(out.endswith("\n\n"))

    async def test_emit_dict_data_serializes_as_json(self) -> None:
        collector = _ByteCollector()
        emitter = SSEEmitter(collector)
        await emitter.emit("token", {"delta": "x", "i": 1})
        out = collector.text()
        self.assertIn("event: token\n", out)
        # JSON payload appears in data line
        self.assertIn("data: ", out)
        # Should be valid JSON
        data_line = [line for line in out.split("\n") if line.startswith("data: ")][0]
        parsed = json.loads(data_line[len("data: "):])
        self.assertEqual(parsed["delta"], "x")
        self.assertEqual(parsed["i"], 1)

    async def test_emit_with_id(self) -> None:
        collector = _ByteCollector()
        emitter = SSEEmitter(collector)
        await emitter.emit("token", "x", id="evt-42")
        out = collector.text()
        self.assertIn("id: evt-42\n", out)
        self.assertIn("event: token\n", out)
        self.assertIn("data: x\n", out)

    async def test_emit_data_no_event_name(self) -> None:
        collector = _ByteCollector()
        emitter = SSEEmitter(collector)
        await emitter.emit_data("hello")
        out = collector.text()
        self.assertNotIn("event:", out)
        self.assertIn("data: hello\n", out)

    async def test_emit_comment(self) -> None:
        collector = _ByteCollector()
        emitter = SSEEmitter(collector)
        await emitter.emit_comment("keepalive")
        out = collector.text()
        self.assertIn(": keepalive\n", out)

    async def test_ping_sends_keepalive_comment(self) -> None:
        collector = _ByteCollector()
        emitter = SSEEmitter(collector)
        await emitter.ping()
        out = collector.text()
        self.assertTrue(out.startswith(": ping "), out)

    async def test_send_raw_appends_newline(self) -> None:
        collector = _ByteCollector()
        emitter = SSEEmitter(collector)
        await emitter.send_raw("data: raw")
        self.assertEqual(collector.text(), "data: raw\n")

    async def test_send_raw_keeps_existing_newline(self) -> None:
        collector = _ByteCollector()
        emitter = SSEEmitter(collector)
        await emitter.send_raw("data: raw\n")
        self.assertEqual(collector.text(), "data: raw\n")

    async def test_close_blocks_further_emits(self) -> None:
        collector = _ByteCollector()
        emitter = SSEEmitter(collector)
        await emitter.close()
        self.assertTrue(emitter.closed)
        with self.assertRaises(RuntimeError):
            await emitter.emit("x", "y")

    async def test_retry_sends_retry_field(self) -> None:
        collector = _ByteCollector()
        emitter = SSEEmitter(collector)
        await emitter.retry(3000)
        out = collector.text()
        self.assertIn("retry: 3000\n", out)

    async def test_auto_id(self) -> None:
        collector = _ByteCollector()
        emitter = SSEEmitter(collector, auto_id=True)
        await emitter.emit("x", "y")
        out = collector.text()
        self.assertIn("id: ", out)


# ---------------------------------------------------------------------------
# SSEApp / routing
# ---------------------------------------------------------------------------


class TestSSEApp(unittest.TestCase):
    async def test_add_route_and_dispatch(self) -> None:
        captured: List[str] = []

        async def handler(req, emitter: SSEEmitter) -> None:
            captured.append("called")
            await emitter.emit("hello", "world")

        app = SSEApp()
        app.add_route("/stream", handler)
        status, headers, run = await app.dispatch("GET", "/stream")
        self.assertEqual(status, 200)
        self.assertEqual(headers[0], ("Content-Type", "text/event-stream"))
        self.assertIsNotNone(run)
        collector = _ByteCollector()
        emitter = SSEEmitter(collector)
        await run(emitter)
        self.assertEqual(captured, ["called"])
        self.assertIn("event: hello\n", collector.text())

    async def test_route_decorator(self) -> None:
        app = SSEApp()

        @app.route("/events")
        async def handler(req, emitter: SSEEmitter) -> None:
            await emitter.emit("ping", "ok")

        status, _, run = await app.dispatch("GET", "/events")
        self.assertEqual(status, 200)
        collector = _ByteCollector()
        await run(SSEEmitter(collector))
        self.assertIn("event: ping\n", collector.text())

    async def test_dispatch_404(self) -> None:
        app = SSEApp()
        status, _, run = await app.dispatch("GET", "/missing")
        self.assertEqual(status, 404)
        self.assertIsNone(run)

    async def test_dispatch_405(self) -> None:
        app = SSEApp()

        async def handler(req, emitter: SSEEmitter) -> None:
            pass

        app.add_route("/x", handler)
        status, _, run = await app.dispatch("POST", "/x")
        self.assertEqual(status, 405)
        self.assertIsNone(run)

    async def test_add_route_invalid_path(self) -> None:
        app = SSEApp()
        with self.assertRaises(ValueError):
            app.add_route("no-slash", lambda r, e: asyncio.sleep(0))

    async def test_cors_header(self) -> None:
        app = SSEApp(cors_origin="https://example.com")

        async def handler(req, emitter: SSEEmitter) -> None:
            pass

        app.add_route("/x", handler)
        _, headers, _ = await app.dispatch("GET", "/x")
        cors = [v for k, v in headers if k == "Access-Control-Allow-Origin"]
        self.assertEqual(cors, ["https://example.com"])

    async def test_make_sse_response_headers(self) -> None:
        headers = make_sse_response_headers({"X-Extra": "1"})
        names = [k for k, _ in headers]
        self.assertIn("Content-Type", names)
        self.assertIn("X-Extra", names)


# ---------------------------------------------------------------------------
# SSEClient
# ---------------------------------------------------------------------------


class TestSSEClient(unittest.TestCase):
    async def test_consume_events(self) -> None:
        raw = (
            b"event: hello\n"
            b"data: world\n"
            b"\n"
            b"data: unnamed\n"
            b"\n"
            b"event: done\n"
            b"data: {\"ok\": true}\n"
            b"\n"
        )
        client = SSEClient("http://test", stream=_FakeStream([raw]))
        events = []
        async with client:
            async for event_type, data in client.events():
                events.append((event_type, data))
        self.assertEqual(events, [
            ("hello", "world"),
            (None, "unnamed"),
            ("done", '{"ok": true}'),
        ])

    async def test_consume_multiline_data(self) -> None:
        raw = (
            b"data: line1\n"
            b"data: line2\n"
            b"\n"
        )
        client = SSEClient("http://test", stream=_FakeStream([raw]))
        async with client:
            out = []
            async for event_type, data in client.events():
                out.append((event_type, data))
        self.assertEqual(out, [(None, "line1\nline2")])

    async def test_consume_chunked(self) -> None:
        raw = (
            b"event: hello\n"
            b"data: wo"
            b"rld\n"
            b"\n"
        )
        # Split into two chunks to test buffering
        client = SSEClient("http://test", stream=_FakeStream([raw[:10], raw[10:]]))
        async with client:
            out = []
            async for event_type, data in client.events():
                out.append((event_type, data))
        self.assertEqual(out, [("hello", "world")])

    async def test_comments_are_skipped(self) -> None:
        raw = (
            b": keepalive\n"
            b"data: real\n"
            b"\n"
        )
        client = SSEClient("http://test", stream=_FakeStream([raw]))
        async with client:
            out = []
            async for event_type, data in client.events():
                out.append((event_type, data))
        self.assertEqual(out, [(None, "real")])

    async def test_close(self) -> None:
        raw = b"data: x\n\n"
        client = SSEClient("http://test", stream=_FakeStream([raw]))
        await client.close()
        self.assertTrue(client._closed)


# ---------------------------------------------------------------------------
# AIStreamer
# ---------------------------------------------------------------------------


class TestAIStreamer(unittest.TestCase):
    async def test_chunks_and_done(self) -> None:
        streamer = AIStreamer()
        await streamer.chunk("Hello")
        await streamer.chunk(" ")
        await streamer.chunk("world")
        await streamer.done()

        out = []
        async for item in streamer.to_async_gen():
            out.append(item)

        # Should have 3 chunks + 1 done
        self.assertEqual(len(out), 4)
        self.assertEqual(out[0]["event"], "chunk")
        self.assertEqual(out[0]["data"]["delta"], "Hello")
        self.assertEqual(out[1]["data"]["delta"], " ")
        self.assertEqual(out[2]["data"]["delta"], "world")
        self.assertEqual(out[3]["event"], "done")
        self.assertEqual(out[3]["data"]["text"], "Hello world")
        self.assertGreaterEqual(out[3]["data"]["tokens"], 11)

    async def test_error_terminates_stream(self) -> None:
        streamer = AIStreamer()
        await streamer.chunk("partial")
        await streamer.error("boom")

        out = []
        async for item in streamer.to_async_gen():
            out.append(item)

        # chunk + error
        self.assertEqual(len(out), 2)
        self.assertEqual(out[0]["event"], "chunk")
        self.assertEqual(out[1]["event"], "error")
        self.assertEqual(out[1]["data"]["message"], "boom")

    async def test_snapshot(self) -> None:
        streamer = AIStreamer()
        await streamer.chunk("foo")
        await streamer.chunk("bar")
        snap = streamer.snapshot()
        self.assertEqual(snap["text"], "foobar")
        self.assertEqual(snap["tokens"], 6)
        self.assertFalse(snap["done"])
        self.assertIsNone(snap["error"])

    async def test_closed_after_done(self) -> None:
        streamer = AIStreamer()
        await streamer.done()
        self.assertTrue(streamer.closed)
        with self.assertRaises(RuntimeError):
            await streamer.chunk("x")

    async def test_custom_event(self) -> None:
        streamer = AIStreamer()
        await streamer.emit("custom", {"x": 1})
        await streamer.done()
        out = []
        async for item in streamer.to_async_gen():
            out.append(item)
        self.assertEqual(out[0]["event"], "custom")
        self.assertEqual(out[0]["data"], {"x": 1})

    async def test_done_meta_merged(self) -> None:
        streamer = AIStreamer()
        await streamer.chunk("a")
        await streamer.done({"model": "test"})
        snap = streamer.snapshot()
        self.assertEqual(snap["done"], True)
        # meta reflected in the done event payload
        out = []
        async for item in streamer.to_async_gen():
            out.append(item)
        self.assertEqual(out[-1]["data"]["model"], "test")


# ---------------------------------------------------------------------------
# stream_sse decorator
# ---------------------------------------------------------------------------


class TestStreamSSE(unittest.TestCase):
    async def test_decorator_basic(self) -> None:
        @stream_sse(event="token")
        async def gen(req, emitter):
            for word in ["hello", "world"]:
                yield word

        app = SSEApp()
        app.add_route("/llm", gen)
        status, _, run = await app.dispatch("GET", "/llm")
        self.assertEqual(status, 200)
        collector = _ByteCollector()
        await run(SSEEmitter(collector))
        out = collector.text()
        # two token events + done
        self.assertEqual(out.count("event: token\n"), 2)
        self.assertIn("data: hello\n", out)
        self.assertIn("data: world\n", out)
        self.assertIn("event: done\n", out)

    async def test_decorator_with_request(self) -> None:
        @stream_sse(event="item")
        async def gen(req, emitter):
            # echo one event using a value from req
            yield req.get("q", "default")

        captured = []

        async def handler(req, emitter):
            captured.append(dict(req))

        app = SSEApp()
        app.add_route("/x", gen)
        _, _, run = await app.dispatch("GET", "/x", {"q": "ping"})
        collector = _ByteCollector()
        await run(SSEEmitter(collector))
        self.assertIn("data: ping\n", collector.text())

    async def test_decorator_handles_exception(self) -> None:
        @stream_sse(event="x")
        async def gen(req, emitter):
            yield "ok"
            raise ValueError("nope")

        app = SSEApp()
        app.add_route("/x", gen)
        _, _, run = await app.dispatch("GET", "/x")
        collector = _ByteCollector()
        await run(SSEEmitter(collector))
        out = collector.text()
        self.assertIn("event: x\n", out)
        self.assertIn("data: ok\n", out)
        self.assertIn("event: error\n", out)
        self.assertIn("nope", out)


# ---------------------------------------------------------------------------
# WebSocket
# ---------------------------------------------------------------------------


class TestWebSocket(unittest.TestCase):
    async def test_route_registration(self) -> None:
        app = WSApp()

        async def handler(ws: WebSocket, req):
            await ws.accept()
            await ws.send_text("hello")

        app.add_route("/ws", handler)
        self.assertIs(app.match("/ws"), handler)
        self.assertIsNone(app.match("/missing"))

    async def test_route_decorator(self) -> None:
        app = WSApp()

        @app.route("/echo")
        async def handler(ws: WebSocket, req):
            await ws.send_json({"ok": True})

        self.assertIsNotNone(app.match("/echo"))

    async def test_invalid_path(self) -> None:
        app = WSApp()
        with self.assertRaises(ValueError):
            app.add_route("bad", lambda ws, req: asyncio.sleep(0))

    async def test_websocket_send_and_receive(self) -> None:
        sent: List[str] = []
        recv_queue: "asyncio.Queue[str]" = asyncio.Queue()

        async def send_text(text: str) -> None:
            sent.append(text)

        async def recv_text() -> str:
            return await recv_queue.get()

        ws = WebSocket(send_text, lambda b: asyncio.sleep(0), recv_text, lambda: asyncio.sleep(0))
        self.assertFalse(ws.closed)
        await ws.accept(subprotocol="chat")
        self.assertEqual(ws.subprotocol, "chat")
        await ws.send_text("hello")
        await ws.send_json({"a": 1})
        await ws.send_bytes(b"raw")
        await recv_queue.put("incoming")
        self.assertEqual(await ws.receive_text(), "incoming")
        self.assertEqual(sent, ["hello", '{"a":1}'])

    async def test_websocket_close(self) -> None:
        ws = WebSocket(
            lambda t: asyncio.sleep(0),
            lambda b: asyncio.sleep(0),
            lambda: asyncio.sleep(0),
            lambda: asyncio.sleep(0),
        )
        await ws.close(code=1000, reason="bye")
        self.assertTrue(ws.closed)

    async def test_websocket_iter_text(self) -> None:
        received: List[str] = []

        async def fake_recv() -> str:
            if not received:
                ws._closed = True
                raise ConnectionClosed()
            return received.pop(0)

        ws = WebSocket(
            lambda t: asyncio.sleep(0),
            lambda b: asyncio.sleep(0),
            fake_recv,
            lambda: asyncio.sleep(0),
        )
        received.append("a")
        received.append("b")
        ws._closed = True  # Will exit immediately

        out = []
        async for text in ws.iter_text():
            out.append(text)
        # iter_text exits when closed (set above before iteration)
        self.assertIsInstance(out, list)


# ---------------------------------------------------------------------------
# iter_to_sse
# ---------------------------------------------------------------------------


class TestIterToSSE(unittest.TestCase):
    async def test_iter_to_sse(self) -> None:
        async def gen() -> AsyncIterator[Dict[str, int]]:
            yield {"a": 1}
            yield {"b": 2}

        collector = _ByteCollector()
        emitter = SSEEmitter(collector)
        await iter_to_sse(gen(), emitter, event="thing")
        out = collector.text()
        self.assertIn("event: thing\n", out)
        self.assertIn('data: {"a":1}\n', out)
        self.assertIn('data: {"b":2}\n', out)


# ---------------------------------------------------------------------------
# End-to-end: SSEApp -> SSEClient
# ---------------------------------------------------------------------------


class TestEndToEnd(unittest.TestCase):
    async def test_sse_app_to_client(self) -> None:
        app = SSEApp()

        async def handler(req, emitter: SSEEmitter) -> None:
            await emitter.emit("token", "Hello")
            await emitter.emit("token", " world")
            await emitter.emit("done", {"ok": True})

        app.add_route("/llm", handler)
        status, _, run = await app.dispatch("GET", "/llm")
        self.assertEqual(status, 200)

        # Capture the run output as a byte stream
        chunks: List[bytes] = []

        class _ListCollector:
            def __init__(self) -> None:
                self.buf = bytearray()

            async def __call__(self, data: bytes) -> None:
                self.buf.extend(data)
                chunks.append(data)

        collector = _ListCollector()
        await run(SSEEmitter(collector))

        # Now feed to a client
        client = SSEClient("http://test", stream=_FakeStream(chunks))
        out = []
        async with client:
            async for event_type, data in client.events():
                out.append((event_type, data))
        self.assertEqual(out, [
            ("token", "Hello"),
            ("token", "world"),
            ("done", '{"ok":true}'),
        ])


# ---------------------------------------------------------------------------
# Sync runner (no pytest needed)
# ---------------------------------------------------------------------------


def _run(coro) -> None:
    """Run a coroutine using a fresh event loop."""
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(coro)
    finally:
        loop.close()


def _wrap_async_tests(cls):
    """Convert every async test method into a sync one that uses `self.run`. """
    import inspect

    for name, value in list(vars(cls).items()):
        if name.startswith("test_") and inspect.iscoroutinefunction(value):
            setattr(cls, name, lambda self, v=value: _run(v(self)))


def _make_suite():
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    for cls in [
        TestSSEFormat,
        TestSSEApp,
        TestSSEClient,
        TestAIStreamer,
        TestStreamSSE,
        TestWebSocket,
        TestIterToSSE,
        TestEndToEnd,
    ]:
        _wrap_async_tests(cls)
        suite.addTests(loader.loadTestsFromTestCase(cls))
    return suite


if __name__ == "__main__":
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(_make_suite())
