# Python version

## Current state

cdip-routing targets **Python 3.10** (since 2026-10, PR "Resolve ER v2 choice
values to SMART option keys"):
- `docker/Dockerfile` → `python:3.10-slim`
- `requirements.txt` is compiled with `uv pip compile --python-version 3.10`
- CI (`.github/workflows/_tests.yml`) runs the suite on 3.10 to match

The move off 3.8 was forced by smartconnect-client 1.15 (`requires-python >= 3.10`),
which routing needs for `resolve_option_key`. The same change lifted httpx to 0.28
and the chain it drags (FastAPI, Starlette, uvicorn, OpenTelemetry, gundi-client,
gundi-client-v2, redis-py 6).

## Why not 3.11 yet

`aioredis==2.0.1` (archived upstream, last release 2021) declares its timeout
exception with both `asyncio.TimeoutError` and the builtin `TimeoutError` as
bases. Python 3.11 made those the same class, so the package fails at import
with `TypeError: duplicate base class TimeoutError`. Nothing in routing's code
triggers it; importing the package is enough.

## Path to 3.11

redis-py absorbed aioredis in 4.2 and routing already carries redis-py 6, so:

1. `app/core/utils.py`: replace `aioredis.from_url(...)` with
   `redis.asyncio.from_url(...)` (same arguments); drop `aioredis` from
   `requirements.in`. Every method routing calls (get, set, setex, incr, decr,
   expire, pipeline) has the same async signature; explicit closes become
   `aclose()`.
2. `docker/Dockerfile` → `python:3.11-slim`; `.python-version` → `3.11`.
3. `uv pip compile --python-version 3.11 -o requirements.txt requirements.in`.
4. `.github/workflows/_tests.yml` → `python-version: '3.11'`; update `AGENTS.md`.
5. Run `pytest` and smoke the Redis client against a real server once.

Do it as its own PR so the interpreter change can be reverted independently.
