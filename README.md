# tiny-realtime — Zero-Dependency SSE + WebSocket Helpers for AI Streaming

> **Server-Sent Events and WebSocket helpers for AI agent streaming. Zero dependencies.**

```bash
pip install tiny-realtime   # coming soon
```

## Why?

- **LLM responses are streaming** — but there's no zero-dep helper to push
  SSE events from a Python service
- **`websockets`** is great but it's a dependency
- **`sse-starlette`** and **`fastapi`** are frameworks, not libraries

**tiny-realtime** gives you SSE emitters, WebSocket helpers, and an AIStreamer —
in one 561-line file, no external packages.

## ✨ Features

- **🪶 Zero dependencies** — stdlib `asyncio`, `http`, `uuid`, `json` only
- **📦 Single file** — 561 lines, drop it anywhere
- **📡 Server-Sent Events** — `SSEApp`, `SSEEmitter`, `stream_sse` decorator
- **🔌 WebSocket** — `WSApp`, `WebSocket` class with `send_text/send_json/send_bytes`
- **🤖 AIStreamer** — buffer and yield LLM token chunks as SSE/WS events
- **📥 SSEClient** — async context manager for consuming SSE endpoints
- **🧩 WSGI-compatible** — works with any WSGI host

## 🚀 Quick Start

### SSE: Stream LLM tokens

```python
from tiny_realtime import SSEApp, SSEEmitter, AIStreamer, stream_sse

app = SSEApp()

@app.route("/stream/{model_id}")
async def stream_model(req, emitter: SSEEmitter, model_id: str):
    streamer = AIStreamer(emitter)

    # Simulate LLM token streaming
    async for token in llm_client.stream(model_id):
        streamer.chunk(token)
    streamer.done()

# Or use the decorator for async generators:
@stream_sse
async def llm_stream(req) -> SSEEmitter:
    emitter = SSEEmitter(req)
    async for token in llm_client.stream("gpt-4"):
        emitter.emit("token", token)
    emitter.emit("done", "complete")
    return emitter
```

### SSE: Push agent heartbeat updates

```python
@app.route("/agent/{agent_id}/events")
async def agent_events(req, emitter: SSEEmitter, agent_id: str):
    # Ping every 30s to keep connection alive
    emitter.ping()

    async for event in agent.walk_events():
        emitter.emit(event.type, event.payload)
        await asyncio.sleep(0.1)
```

### SSEClient: Consume an SSE stream

```python
from tiny_realtime import SSEClient

async with SSEClient("https://api.example.com/stream") as client:
    async for event_type, data in client.events():
        print(f"[{event_type}] {data}")
```

### WebSocket: Bidirectional agent communication

```python
from tiny_realtime import WSApp

ws = WSApp()

@ws.route("/ws/agent/{agent_id}")
async def agent_ws(ws_handler, req, agent_id: str):
    await ws_handler.accept()

    # Receive commands from client
    async for msg in ws_handler.iter_json():
        result = await agent.run(msg)
        await ws_handler.send_json({"status": "ok", "result": result})

    # Send push updates
    async for event in agent.walk_events():
        await ws_handler.send_json(event)
```

### AIStreamer: Buffer and flush token chunks

```python
from tiny_realtime import AIStreamer, SSEEmitter

async def llm_stream(req, emitter: SSEEmitter):
    streamer = AIStreamer(emitter, flush_every=3)  # flush every 3 chunks

    for token in await gpt4.stream(prompt):
        streamer.chunk(token)  # buffered, flushed every 3 tokens

    streamer.done()  # sends "done" event
```

## API Reference

### SSEEmitter

| Method | Description |
|--------|-------------|
| `emit(event, data, id=None)` | Send an SSE event with optional ID |
| `emit_comment(comment)` | Send a comment line |
| `send_raw(line)` | Send a raw SSE-formatted line |
| `ping()` | Send a keepalive comment |
| `close()` | End the stream |

### AIStreamer

| Method | Description |
|--------|-------------|
| `chunk(text)` | Buffer a text chunk (LLM token) |
| `done(message="complete")` | Mark stream as done |
| `error(message)` | Send an error event |
| `flush()` | Force flush the buffer |
| `to_async_gen()` | Convert buffer to async generator |

### SSEClient

| Method | Description |
|--------|-------------|
| `events()` | Async iterator yielding `(event_type, data)` tuples |
| `close()` | Close the connection |

### WSApp / WebSocket

| Class | Description |
|-------|-------------|
| `WSApp` | WebSocket router |
| `WSApp.add_route(path, handler)` | Register a WS endpoint |
| `WebSocket.accept(subprotocol)` | Accept upgrade |
| `WebSocket.send_text(text)` | Send text message |
| `WebSocket.send_json(obj)` | Send JSON message |
| `WebSocket.send_bytes(data)` | Send binary message |
| `WebSocket.send_ping(data)` | Send ping frame |
| `WebSocket.receive_text()` | Receive one text message |
| `WebSocket.receive_bytes()` | Receive one binary message |
| `WebSocket.iter_text()` | Async iterator over text messages |
| `WebSocket.iter_json()` | Async iterator over JSON messages |
| `WebSocket.close(code, reason)` | Close the connection |

### Decorator

| Decorator | Description |
|-----------|-------------|
| `@stream_sse` | Wrap an async generator as an SSE endpoint handler |

## Agent Workflow Fit

`tiny-realtime` is essential for any AI agent that needs to push live updates:

- **LLM response streaming** — stream token-by-token to a web UI or CLI
- **Agent heartbeat** — push status updates every N seconds to dashboards
- **Tool progress** — stream intermediate steps as events
- **Webhook push** — send real-time notifications to connected clients
- **Multi-agent coordination** — WebSocket pub/sub between agents

Pair it with `tiny-router` for HTTP routing, `tiny-validator` for payload validation,
`tiny-log` for structured output, and `tiny-eventbus` for durable event history.

## Ecosystem

Part of the **tiny-*** zero-dependency toolkit for Python agent infrastructure:

- [**tiny-router**](https://github.com/hussain-alsaibai/tiny-router) — HTTP router, 76K req/s
- [**tiny-log**](https://github.com/hussain-alsaibai/tiny-log) — structured logging
- [**tiny-validator**](https://github.com/hussain-alsaibai/tiny-validator) — input validation, 247K val/s
- [**tiny-config**](https://github.com/hussain-alsaibai/tiny-config) — layered config loader
- [**tiny-cli**](https://github.com/hussain-alsaibai/tiny-cli) — CLI builder with colors
- [**fast-cache**](https://github.com/hussain-alsaibai/fast-cache) — LRU + TTL + SWR cache
- [**tiny-rate**](https://github.com/hussain-alsaibai/tiny-rate) — rate limiter (token / fixed / sliding)
- [**tiny-retry**](https://github.com/hussain-alsaibai/tiny-retry) — retry + backoff + circuit breaker
- [**tiny-pool**](https://github.com/hussain-alsaibai/tiny-pool) — ThreadPool + AsyncPool
- [**tiny-agent**](https://github.com/hussain-alsaibai/tiny-agent) — zero-dep agent framework
- [**tiny-mcp**](https://github.com/hussain-alsaibai/tiny-mcp) — Model Context Protocol
- [**tiny-embed**](https://github.com/hussain-alsaibai/tiny-embed) — embeddings + vector search
- [**tiny-compose**](https://github.com/hussain-alsaibai/tiny-compose) — Stack any decorators in any order, declaratively
- [**tiny-trace**](https://github.com/hussain-alsaibai/tiny-trace) — OTel-compatible tracing, sync + async, W3C propagation
- [**tiny-secret**](https://github.com/hussain-alsaibai/tiny-secret) — Zero-dep secret loader + redacting printer
- [**tiny-cron**](https://github.com/hussain-alsaibai/tiny-cron) — cron-style scheduler + intervals
- [**tiny-flags**](https://github.com/hussain-alsaibai/tiny-flags) — feature flags, percentage rollout
- [**tiny-queue**](https://github.com/hussain-alsaibai/tiny-queue) — persistent FIFO queue, retries
- [**tiny-metrics**](https://github.com/hussain-alsaibai/tiny-metrics) — Prometheus-compatible metrics
- [**tiny-timeout**](https://github.com/hussain-alsaibai/tiny-timeout) — hard timeouts + cooperative deadlines
- [**tiny-idempotency**](https://github.com/hussain-alsaibai/tiny-idempotency) — Stripe-style idempotency keys
- [**tiny-budget**](https://github.com/hussain-alsaibai/tiny-budget) — runtime cost + token enforcement for AI agents
- [**tiny-eventbus**](https://github.com/hussain-alsaibai/tiny-eventbus) — durable pub/sub with JSONL replay
- [**tiny-semaphore**](https://github.com/hussain-alsaibai/tiny-semaphore) — async semaphore with timeout + fairness
- [**tiny-circuit**](https://github.com/hussain-alsaibai/tiny-circuit) — circuit breaker for fault tolerance
- [**tiny-realtime**](https://github.com/hussain-alsaibai/tiny-realtime) — SSE + WebSocket helpers for AI agent streaming
- [**snapdb**](https://github.com/hussain-alsaibai/snapdb) — embedded DB

28 repos, zero dependencies across the entire stack. All single-file, MIT, fully type-hinted. Built by [OpenClaw](https://github.com/hussain-alsaibai).

## License

MIT — see [LICENSE](LICENSE).
