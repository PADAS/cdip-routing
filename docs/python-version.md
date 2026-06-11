# Python version

## Current state

cdip-routing targets **Python 3.8**:
- `docker/Dockerfile` → `python:3.8-slim`
- `requirements.txt` is compiled with `uv pip compile --python-version 3.8`
- CI (`.github/workflows/_tests.yml`) runs the suite on 3.8 to match

## Why 3.8? (evaluated 2026-06-11)

**It's inertia, not a hard requirement.** The Docker image was set to Python 3.8 in commit `599833e` ("Update docker image for FastAPI", 2024-02-20) and hasn't moved since. Investigation found nothing that actually requires < 3.9:

- No `python_requires` / `requires-python` constraint in `requirements.in`, `setup.py`, or `pyproject.toml`.
- No code uses `zoneinfo` / `backports.zoneinfo` directly (`grep` across `app/` is empty).
- `backports-zoneinfo==0.2.1` in the lockfile is a **transitive** dependency that only appears **because the lockfile is compiled for 3.8** — it's gated on `python_version < "3.9"`. It's a *symptom* of the pin, not a reason for it. (It's also the dep that breaks `pip install` on 3.10+, since it has no wheel there and fails to build — which is why CI pins 3.8 today.)

So the 3.8 pin is self-reinforcing: the lockfile targets 3.8 → pulls `backports.zoneinfo` → which only installs on ≤3.8 → so we stay on 3.8.

## Risk

**Python 3.8 reached end-of-life on 2024-10-07** — no security or bug fixes. The sibling Gundi integration runners (gundi-integration-cmore, gundi-integration-earthranger) already run 3.10/3.11.

## Recommendation

Upgrade to **Python 3.11** (aligns with the integration runners) as a focused follow-up:

1. `docker/Dockerfile`: `python:3.11-slim`.
2. Recompile the lockfile: `uv pip compile --python-version 3.11 -o requirements.txt requirements.in` — this **drops `backports-zoneinfo`** automatically.
3. Bump `python-version` in `.github/workflows/_tests.yml` to `'3.11'`.
4. Verify the pinned first-party wheels install on 3.11 (`earthranger_client`, `smartconnect_client` — both appear to be `py3-none-any`, but confirm) and run `pytest`.

Not done here because it's a distinct change with its own validation; tracked as a follow-up. Until then, keep CI on 3.8 so it matches the deployed image.
