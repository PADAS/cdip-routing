# AGENTS.md

Guidance for AI coding agents working in this repository. This is the canonical file; `CLAUDE.md`
is a one-line pointer to it, so Claude Code, Codex and anything else that reads `AGENTS.md` all get
the same instructions. Edit this file, never the pointer.

## What this service is

`cdip-routing` is Gundi's **routing/transformation service**: a FastAPI app deployed on Cloud Run as a
**Pub/Sub push endpoint**. Google Pub/Sub POSTs a message envelope to `/`; the service decodes it,
looks up connection/destination configuration in the Gundi portal, transforms the observation into each
destination's schema, and publishes the result to that destination's Pub/Sub topic, where a dispatcher
or action runner picks it up.

There is no consumer loop and no worker process — **one HTTP request = one Pub/Sub message**. Raising an
exception out of the handler is how a message gets retried by GCP; returning normally acks it.

> `README.md` is stale: it describes Kafka subscribers, `app/subscribers/` and `app/transform_service/`,
> none of which exist anymore. Kafka is dead: the `Broker.KAFKA` member and the `supported_brokers` set in
> `app/core/utils.py` are unused leftovers — nothing reads either, so don't preserve them as load-bearing.
> Rejection compares the portal's `additional.broker` string against `Broker.GCP_PUBSUB.value`.
> Trust the code over the README.

## Commands

```bash
# One-time setup: the suite needs the pinned deps in a 3.8 venv
python3.8 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt

# Tests (no pytest.ini/pyproject; pytest is run bare from the repo root)
TRACING_ENABLED=false pytest
TRACING_ENABLED=false pytest app/tests/test_process_observations_v2.py
TRACING_ENABLED=false pytest app/tests/test_process_request.py::test_message_v2_deduplication

# Run locally (needs .env — copy .env-example — plus a reachable Redis and portal)
uvicorn app.main:app --host 0.0.0.0 --port 8080 --reload

# Send a sample Pub/Sub push envelope at a locally running service.
# test_local.sh is a curl scratchpad: one active payload — itself an
# AttachmentReceived v2 message — plus many commented-out ones (EventReceived,
# EventUpdateReceived per destination, v1 messages...).
# Uncomment the case you want; it POSTs to localhost:8282, so run uvicorn on that
# port or edit the script.
./test_local.sh

# Formatting/secret scanning (black, detect-secrets, ggshield)
pre-commit run --all-files

# Build + push the image manually (CI normally does this).
# The target lives in GNUmakefile and needs GNU make >= 4.1 — macOS ships 3.81,
# so use `gmake build_and_push` (brew install make) there.
make build_and_push
```

`TRACING_ENABLED=false` matters locally and in CI: `app/core/tracing` configures the GCP CloudTrace
exporter **at import time** (via `app/__init__.py`), so without it, tests fail with
`DefaultCredentialsError` on a machine with no GCP auth.

**Python 3.8** — matched by `docker/Dockerfile`, the compiled `requirements.txt`, and CI. See
`docs/python-version.md`: the pin is inertia, not a requirement, and the upgrade path to 3.11 is written up
there. Don't add 3.9+ syntax without doing that upgrade.

Dependencies are pinned in `requirements.in` and compiled to `requirements.txt` with
`uv pip compile --python-version 3.8`. Edit the `.in` file, never `requirements.txt` by hand.

## Request flow

`app/main.py` → `process_request` (`app/services/process_messages.py`) does the envelope-level work
before any routing:

1. `extract_fields_from_message` base64-decodes `message.data` into a payload dict + `attributes`.
2. Tracing context is restored from `attributes["tracing_context"]`.
3. **Dedup**: `message_id = payload.event_id or pubsub message_id`; if seen in Redis
   (`app/core/deduplication.py`, TTL `EVENT_PROCESSING_STATUS_TTL`) → dead-letter + discard.
4. **Age check**: `is_too_old` against `MAX_EVENT_AGE_SECONDS` → dead-letter + discard. This is the
   retry-limit mechanism; Pub/Sub retries the same message until it ages out. **Known bug**: the
   implementation uses `timedelta.seconds`, which drops whole days, so a 25-hour-old message reads as one
   hour old and is never discarded under the 24h default. `total_seconds()` is the fix.
5. Branch on `attributes["gundi_version"]`:
   - **v1** → `process_observation` (legacy; `OutboundConfiguration` per device, no system-event envelope)
   - **v2** → `process_observation_event` → `event_handlers[event_type]`

### v2 routing (the path that matters)

`app/services/event_handlers.py` is the core. `event_handlers` / `event_schemas` map a gundi_core system
event name (`ObservationReceived`, `ObservationsBatchReceived`, `EventReceived`, `EventUpdateReceived`,
`AttachmentReceived`, `TextMessageReceived`) to a handler and a pydantic schema. Adding a new system event
means adding to *both* dicts.

`transform_and_route_observation` is the single-item path:

1. `get_connection(data_provider_id)` → provider + destinations; `get_route(default_route.id)` → route
   configuration (field mappings).
2. `provider_key = get_provider_key(provider)` → `gundi_<type>_<provider_id>`.
3. For each destination: `get_integration(destination.id)` → `additional` is the **broker config**
   (`broker`, `topic`, legacy `generic_model` flag).
4. Two publish paths:
   - **Generic-model** (`_uses_generic_model`): wrap the untransformed payload in a `GundiDelivery`
     envelope and publish; the destination's *action runner* does the transformation. Selected by
     destination integration *type* via `settings.GENERIC_MODEL_DESTINATION_TYPES` (default `["cmore"]`).
     `additional.generic_model` is a one-off **opt-in** for a type that isn't on the list; it cannot opt a
     listed type *out*, since the type check runs first.
   - **Legacy in-process**: `transform_observation_v2` picks a `Transformer` class out of
     `transformers_map[stream_type][destination_type]` in `app/services/transformers.py`, applies route
     field-mapping rules, then wraps the result in the matching `*Transformed*` system event via
     `transformer_events_by_data_type` and publishes it. **Exception**: a transformer that returns a plain
     `dict` (e.g. Movebank) is published raw, unwrapped — older dispatchers depend on that shape.
5. `build_transformed_message_attributes` (in `transformers.py`) builds the Pub/Sub attributes the
   dispatcher reads. The **ordering key is separate** — it is computed at the publish call in
   `event_handlers.py`, not by that function — and is set to `gundi_id` **only for `event_update`**;
   creates publish with an empty key. Pub/Sub only orders messages sharing a non-empty key, so this
   serializes updates against each other — it does **not** guarantee an update arrives after its create.

`transform_and_route_observations_batch` is the batch path for `ObservationsBatchReceived`. Its invariants,
which are easy to break:

- **One** connection/route lookup for the whole batch — every item shares the provider by envelope invariant.
- Broker validation is hoisted **before** any transform/publish work per destination, so a batch cannot
  slip an unsupported broker through. The single-item path is **not** equivalent: it checks *after*
  `transform_observation_v2` and after attribute building, and its `broker_config.get("broker", …).strip()`
  has no null guard — so `additional.broker: null` raises `AttributeError` (an unexpected-error retry)
  rather than the documented `ReferenceDataError`. The batch path's `str(… or …)` guard exists for exactly
  that portal record.
- A failing **transform** is dropped, never aborts the batch ("shrink the batch, never abort it"). The
  per-item `try` wraps `transform_observation_v2` and nothing else, so anything it raises — including a
  `ReferenceDataError` from a malformed field mapping — drops just that item. Everything else propagates and
  retries the whole envelope: the connection and route lookups, broker validation, and the `AttributeError`
  on `destination_integration.additional` when `get_integration` returns `None` (it swallows its own errors
  and returns in a `finally`, so it never raises — don't grep it for a `raise`).
- **Everything after the transform sits outside that try**, so the invariant above does not cover it:
  the per-item `send_message_to_gcp_pubsub_dispatcher` for non-ER results, the per-item
  `_publish_gundi_delivery` for generic-model destinations, `_publish_transformed_batch_group`, and also
  `build_transformer_event`, `build_transformed_message_attributes` and the `TransformedERObservationItem`
  construction. A 500-item batch whose item 300 exhausts its `backoff` retries on publish aborts the whole
  batch, and redelivery re-publishes items 1–299. `build_transformer_event` is the sharpest edge: it does
  `transformer_events_by_data_type[type(x).__name__]`, so a new transformer result type missing from that
  map raises `KeyError` and retries the envelope forever — add the map entry with the transformer.
- Items are grouped per **effective `provider_key`** (field mappings can override it per item) because one
  ER bulk post carries exactly one provider_key in its URL path.
- Generic-model destinations and non-ER transform results keep publishing **per item**: splitting a batch
  is allowed, merging never is.

### Error handling contract

- `ReferenceDataError` and unexpected exceptions retry the message **only when they escape the handler** —
  raising out of it is the retry signal to GCP.
- **Anything raised inside `transform_observation_v2` never escapes on the v2 paths.** Both call sites
  (single-item and batch) wrap it in a bare `except Exception` and continue, so a `ReferenceDataError` from
  a malformed route field mapping is *discarded*, not retried — bad portal config fails silently. Don't
  build alerting or tests on the assumption that it retries.
- Transformer errors **discard that destination's item** (v2) or dead-letter it (v1) and continue —
  a bad transform for one destination must not block the others.
- `send_observation_to_dead_letter_topic` targets `settings.DEAD_LETTER_TOPIC`.

## Modules

- `app/core/gundi.py` — all portal access, via `gundi_client.PortalApi` (`_portal`, v1) and
  `gundi_client_v2.GundiClient` (`portal_v2`). Every lookup is **Redis-cached** with
  `PORTAL_CONFIG_OBJECT_CACHE_TTL`. Not caching an empty response is **deliberate**: four lookups carry the
  same `# don't cache empty response` guard (`get_outbound_config_detail`, `get_inbound_integration_detail`,
  `get_all_outbound_configs_for_id`, `ensure_device_integration`) — strip them and you reintroduce negative
  caching of empty portal responses for a full TTL. What is *not* deliberate: `write_to_cache_safe` logs
  "Ignoring null instance" and then falls through with no `return`, so the write throws `AttributeError` and
  the generic `except` swallows it; that one needs the missing `return`. Only the v2 lookups
  (`get_connection`, `get_route`, `get_integration`) emit a portal-visible activity log on failure; the v1
  helpers log and then `raise ReferenceDataError` — which is the retry signal, not a local-only log — and
  `ensure_device_integration` silently returns a blank device.
- `app/services/activity_logger.py` — publishes `IntegrationActionCustomLog` to
  `INTEGRATION_EVENTS_TOPIC` so operators see lookup failures in the portal. **Best-effort by contract**:
  it never raises, and is deduped in Redis for `ACTIVITY_LOG_DEDUP_TTL`.
- `app/core/pubsub.py` — all publishing (`gcloud.aio.pubsub`). Only
  `send_message_to_gcp_pubsub_dispatcher` has `backoff` retries; dead-letter and activity-log publishing
  get one attempt. Destination topic comes from `broker_config["topic"]`, falling back to
  `destination-<id>-<GCP_ENVIRONMENT>`.
- `app/core/settings.py` — settings are `environs` env vars with defaults, read at import; there is no
  settings class. A few values are plain code constants with no env knob (e.g. `DEFAULT_REQUESTS_TIMEOUT`). Note `app/__init__.py` re-exports it, so both `from app import settings` and
  `from app.core import settings` appear in the codebase.
- `app/services/transformers.py` — ~2000 lines of destination-specific `Transformer` subclasses (ER,
  SMART, WPS Watch, TrapTagger, Movebank, InReach). v1 uses the `if/elif` chain in `transform_observation`;
  v2 uses the `transformers_map` dict at the bottom of the file. SMART transformers are the most involved
  (conservation-area resolution, timezone guessing, data-model lookups).

## Tests

`pytest` + `pytest-asyncio` + `pytest-mock`. Every async test is explicitly marked `@pytest.mark.asyncio`
(no `asyncio_mode = auto`). `app/conftest.py` is large (~110 fixtures) and holds all the raw message
payloads and mock clients — **look there before building a new fixture**; there is usually already one for
your stream type, and `async_return()` is the helper for stubbing coroutines.

Tests exercise the real entry points (`process_request`, `process_observation_event`) and patch only at
the boundaries. Use these exact targets — patching elsewhere silently misses, since these are
module-level singletons bound at import:

```python
mocker.patch("app.core.gundi._cache_db", mock_cache)        # Redis
mocker.patch("app.core.gundi.portal_v2", mock_gundi_client_v2)  # portal v2
mocker.patch("app.core.gundi._portal", mock_gundi_client)   # portal v1
mocker.patch("app.core.pubsub.pubsub", mock_pubsub)         # gcloud.aio.pubsub
mocker.patch("app.core.deduplication._cache_db", mock_cache)
mocker.patch("app.services.activity_logger._cache_db", mock_cache)  # portal-error log dedup
```

That last one is easy to miss: every v2 portal-lookup failure reaches `log_portal_lookup_error`, which
does its own `_cache_db.set(...)`. Leave it unpatched and the test opens a real connection to
`localhost:6379`. Patching `app.core.gundi.log_portal_lookup_error` outright works too — see
`test_get_connection_details.py`.

Assertions are typically "did we publish, and with what payload/attributes" —
`mock_pubsub.PublisherClient.return_value.publish.called` and inspecting the decoded message.

## Tests are part of the change

A change is not finished until its tests are. Creating, modifying, or refactoring a handler, transformer,
or portal/pubsub helper means writing or updating the tests that cover it in the same change.

**Do:**

- **Follow the suite that exists.** `pytest` + `pytest-asyncio` + `pytest-mock`, tests in `app/tests/`,
  fixtures in `app/conftest.py`. Every async test carries an explicit `@pytest.mark.asyncio`.
- **Name by behavior, not by source file.** This suite is grouped by what is exercised
  (`test_process_observation_batches_v2.py`, `test_transform_observations_v2.py`,
  `test_get_connection_details.py`), not mirrored 1:1 against `app/`. Extend the file that already covers
  the behavior before creating a new one.
- **Test through the real entry point.** Call `process_request` / `process_observation_event` /
  `transform_and_route_observation` / `transform_and_route_observations_batch`, and patch only at the
  boundaries listed above. Tests that
  call a private helper directly tend to pass while the routing path is broken.
- **Reuse `app/conftest.py`.** There is almost certainly already a raw payload, attributes dict, or mock
  client for your stream type; `async_return()` is the helper for stubbing coroutines. Add a new fixture
  there only when nothing fits.
- **Cover the three outcomes this service actually has**, not just a happy path:
  1. the message is transformed and published (assert the topic, attributes, and decoded payload);
  2. the message is **discarded** — duplicate, too old, unsupported version, transformer error,
     no destinations — assert nothing reached the *destination* topic. Do **not** assert
     `not publish.called`: duplicate, too old and unsupported version all dead-letter, and that publishes
     through the same patched mock, so `publish.called` is `True`. Distinguish by the `topic_path` args
     (`test_message_deduplication` is the model);
  3. the message is **retried** — `ReferenceDataError` or an unexpected error propagates out of the
     handler (`pytest.raises`), because propagating is the retry signal to GCP.
  For batch changes, also assert that one failing item shrinks the batch instead of aborting it, and that
  grouping by effective `provider_key` still holds.

**Don't:**

- **Don't call a task complete without its tests**, and don't leave a behavior change covered only by an
  existing test that happened to keep passing.
- **Don't use `unittest`/`unittest.mock` directly** — use the `mocker` fixture from `pytest-mock`
  (the lone `import unittest.mock` in `app/tests/test_process_request.py` is an unused leftover).
- **Don't let a test touch Redis, the portal, or Pub/Sub.** All three are module-level singletons bound at
  import; patch them at the exact targets above so the suite stays deterministic and offline.
- **Don't add config to make tests pass.** There is no `pytest.ini`/`pyproject.toml` on purpose; if a test
  needs `asyncio_mode` or a plugin setting, mark the test instead.

## Comments and responses

### Don't

- **No preamble, no wrap-up.** Skip "Here's the updated code" and "Hope this helps" — lead with the change
  or the answer.
- **Don't narrate the diff.** Comments describing what the code used to do, or summarizing the edit just
  made, belong in the commit message or the PR, never in the source.
- **Don't comment the obvious.** If the syntax already says it, the comment is noise.
- **Don't strip the existing `why` comments.** Several in this repo encode non-obvious invariants — the
  `str(...) or ...` guard around a null `additional.broker`, why the broker check is hoisted above the
  batch loop, why generic-model destinations publish per item. Removing them as "clutter" loses real
  information; correct them if the code moves.

### Do

- **Answer with the code.** Deliver the solution directly; if the code answers the question, add no prose
  around it.
- **Comment only what the code can't say:** a performance or ordering trade-off, a security or data-isolation
  consequence, an upstream quirk in portal data or a dispatcher contract, or business logic subtle enough
  to be edited wrong later. Write the *why*, not the *what*.
- **Keep it short.** One or two lines above the code, in the surrounding style — lowercase, plain, no
  decorative banners or section dividers.

## CI/CD

- `.github/workflows/_tests.yml` — reusable test job, the single source of truth for how the suite runs.
- `tests.yml` runs it on every PR; `main.yml` runs it as a **gate** before build/deploy.
- `main.yml` builds the image and deploys via Terragrunt: pushes to `main` → **dev**; pushes to
  `release-**` → **stage**, then **prod** once `stage-deploy` succeeds. Infra lives in `terraform/` (Cloud Run, Pub/Sub topics/subscriptions, IAM,
  Secret Manager, monitoring), per-environment under `terraform/environments/{dev,stage,prod}`.
