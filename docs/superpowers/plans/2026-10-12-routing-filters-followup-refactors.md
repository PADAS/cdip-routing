# Routing Filters Follow-up Refactors Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the three duplications the medium-effort review of PR #162 flagged (#4, #5, #6) without changing routing behavior.

**Architecture:** PR #162 (`gundi-5712-filter-enforcement`) enforces device routing filters on the v2 paths in `app/services/event_handlers.py` and publishes `ObservationFiltered` events from `app/core/pubsub.py`. This plan (1) swaps a hand-rolled dict lookup for gundi-core's `Route.filter_for()`, (2) factors the two near-identical system-event publishers behind one private helper, and (3) moves the per-destination filter partition into a helper both the single-item and batch paths call. Every task is behavior-neutral and is pinned by the 20 tests in `app/tests/test_source_filter_enforcement.py` plus the activity-logger tests.

**Tech Stack:** Python 3.8 (3.10 after PR #161 merges — recompile `requirements.txt` if that lands first), FastAPI, pydantic v1, gundi-core 1.14.0, `gcloud.aio.pubsub`, pytest + pytest-asyncio + pytest-mock.

**Spec:** [GUNDI-5804](https://allenai.atlassian.net/browse/GUNDI-5804) (objective, constraints, acceptance criteria) — the findings came from the review of PR #162; the code contract is `gundi_core/schemas/v2/gundi.py` (`Route.filter_for`, `SourceListFilter.allows`) in gundi-core 1.14.0.

## Global Constraints

- Branch off `main` **after** PR #162 merges. Do not push to `gundi-5712-filter-enforcement`.
- Behavior-neutral: no test in `app/tests/test_source_filter_enforcement.py` changes its assertions. Tests may change only which name they patch.
- Run every test as `TRACING_ENABLED=false .venv/bin/python -m pytest ...` (see AGENTS.md: tracing configures the GCP exporter at import).
- Python 3.8 syntax only (no `match`, no `X | Y` types) unless PR #161's 3.10 bump has merged.
- Keep the existing "why" comments in `event_handlers.py` (broker `str(... or ...)` guard, hoisted broker check, per-item generic-model publishing, policy-before-setup); move them with the code, never delete them.
- Format with `uvx black==22.10.0 <files>` (the repo's pinned black; `black` is not on PATH).
- Commit messages end with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.

## Review Focus

1. A destination id arriving as a non-canonical UUID string (uppercase, or un-hyphenated) must still match its rule — Task 1 adds the test.
2. A publish failure on the activity-log path must still be swallowed after the publishers share code — the existing `test_publish_failure_is_swallowed` in `app/tests/test_activity_logger.py` pins it; Task 2 keeps it green.
3. An empty event list must not open a Pub/Sub session — Task 2 adds the test.
4. After Task 3, the single-item path must still set `is_discarded` on its span and the batch path must not — Task 3's test asserts both via the span mock.
5. After Task 3, a batch destination where every item is dropped must still skip `get_integration` — the existing `test_an_all_dropped_batch_skips_destination_setup` pins it.

---

### Task 1: Use `Route.filter_for()` for the rule lookup (#4)

**Files:**
- Modify: `app/services/event_handlers.py:256-273` (`_dropping_source_filter`)
- Test: `app/tests/test_source_filter_enforcement.py`

**Interfaces:**
- Consumes: `gundi_core.schemas.v2.Route.filter_for(destination_id) -> Optional[SourceListFilter]` (gundi-core 1.14.0). It canonicalizes `destination_id` through `str(UUID(str(key)))` the same way the `filters` keys were stored, so any spelling of the same UUID matches.
- Produces: `_dropping_source_filter(route, destination_id, observation)` — unchanged signature.

- [ ] **Step 1: Write the failing test**

Append to `app/tests/test_source_filter_enforcement.py`:

```python
@pytest.mark.asyncio
async def test_a_rule_matches_a_non_canonical_destination_id_spelling(
    mocker, enforcement_mocks, mock_gundi_client_v2, connection_v2, route_v2
):
    # Route.filters keys are canonicalized by gundi-core; the lookup must
    # canonicalize the destination id the same way instead of str()-ing it.
    dispatcher, routing_events = enforcement_mocks
    _enable(mocker)
    provider_id = str(connection_v2.provider.id)
    canonical = str(connection_v2.destinations[0].id)
    connection = connection_v2.copy(deep=True)
    # .construct() skips validation, so the id stays a plain str in this
    # spelling instead of being parsed back into a canonical UUID.
    connection.destinations[0] = type(connection.destinations[0]).construct(
        **{**connection.destinations[0].dict(), "id": canonical.upper().replace("-", "")}
    )
    mock_gundi_client_v2.get_connection_details.return_value = async_return(connection)
    mock_gundi_client_v2.get_route_details.return_value = async_return(
        _route_with_filter(
            route_v2, canonical, provider_id, mode="blacklist", ids=["collar-001"]
        )
    )

    await process_observation_event(
        _observation_event_dict(provider_id, "collar-001"), _ATTRIBUTES
    )

    assert dispatcher.call_count == 0
    assert routing_events.call_count == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `TRACING_ENABLED=false .venv/bin/python -m pytest app/tests/test_source_filter_enforcement.py::test_a_rule_matches_a_non_canonical_destination_id_spelling -q`
Expected: FAIL with `assert 1 == 0` on `dispatcher.call_count` — the un-hyphenated uppercase id does not match the canonical dict key, the rule fails open, and the observation is delivered.

If `get_integration` or the dispatcher mock chokes on the odd id spelling before the assertion is reached, that is a different failure: the mocks in `enforcement_mocks` ignore the id, so re-check that `.construct` kept `id` as a `str` (print `type(connection.destinations[0].id)`).

- [ ] **Step 3: Write minimal implementation**

In `app/services/event_handlers.py`, replace the lookup line in `_dropping_source_filter`:

```python
    rule = route.filter_for(destination_id) if route is not None else None
```

(the previous line was `rule = (getattr(route, "filters", None) or {}).get(str(destination_id))`). Keep everything else in the function as is.

- [ ] **Step 4: Run tests to verify they pass**

Run: `TRACING_ENABLED=false .venv/bin/python -m pytest app/tests/test_source_filter_enforcement.py -q`
Expected: 21 passed.

- [ ] **Step 5: Commit**

```bash
uvx black==22.10.0 app/services/event_handlers.py app/tests/test_source_filter_enforcement.py
git add app/services/event_handlers.py app/tests/test_source_filter_enforcement.py
git commit -m "Look up routing filters through Route.filter_for

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: One system-event publisher behind both topic functions (#5)

**Files:**
- Modify: `app/core/pubsub.py:72-156` (`send_event_to_integration_events_topic`, `ROUTING_EVENTS_PUBLISH_CHUNK`, `send_events_to_routing_events_topic`, `send_event_to_routing_events_topic`)
- Test: `app/tests/test_source_filter_enforcement.py`, `app/tests/test_activity_logger.py` (unchanged, must stay green)

**Interfaces:**
- Consumes: `settings.INTEGRATION_EVENTS_TOPIC`, `settings.INTEGRATION_EVENTS_PUBLISH_TIMEOUT_SECONDS`, `settings.ROUTING_EVENTS_TOPIC`, `settings.ROUTING_EVENTS_PUBLISH_TIMEOUT_SECONDS`.
- Produces:
  - `async def _publish_system_events(events, *, topic_name, timeout_seconds, span_name) -> None` — private; best-effort; chunks at `SYSTEM_EVENTS_PUBLISH_CHUNK = 500`; returns immediately on an empty list.
  - `send_event_to_integration_events_topic(event)`, `send_events_to_routing_events_topic(events)`, `send_event_to_routing_events_topic(event)` — public names and signatures unchanged, now thin wrappers.

- [ ] **Step 1: Write the failing test**

Append to `app/tests/test_source_filter_enforcement.py`:

```python
@pytest.mark.asyncio
async def test_publishing_no_routing_events_opens_no_session(mocker, mock_pubsub):
    from app.core.pubsub import send_events_to_routing_events_topic

    mocker.patch("app.core.pubsub.pubsub", mock_pubsub)
    session = mocker.patch("app.core.pubsub.aiohttp.ClientSession")

    await send_events_to_routing_events_topic([])

    assert not session.called
    assert not mock_pubsub.PublisherClient.called
```

- [ ] **Step 2: Run test to verify it fails**

Run: `TRACING_ENABLED=false .venv/bin/python -m pytest app/tests/test_source_filter_enforcement.py::test_publishing_no_routing_events_opens_no_session -q`
Expected: PASS already — `send_events_to_routing_events_topic` has an `if not events: return` guard. That is fine: this test exists to pin the guard through the refactor, so note "passes before refactor" and continue. (If it fails, the guard was lost in a rebase; restore it before going on.)

- [ ] **Step 3: Write the shared helper and the wrappers**

In `app/core/pubsub.py`, replace everything from `async def send_event_to_integration_events_topic(event):` through the end of `send_event_to_routing_events_topic` (currently lines 72–156) with:

```python
# Pub/Sub caps a publish request at 1000 messages / 10 MB; stay well under both.
SYSTEM_EVENTS_PUBLISH_CHUNK = 500


async def _publish_system_events(events, *, topic_name, timeout_seconds, span_name):
    """Publish gundi_core system events to a topic in as few requests as possible.

    Best-effort by contract: callers use this for records the portal reads
    (activity logs, drop records), never for the observation itself, so a
    failed publish is logged and swallowed - raising would retry the whole
    envelope and re-deliver observations that already reached a destination.
    """
    if not events:
        return
    with tracing.tracer.start_as_current_span(
        span_name, kind=SpanKind.PRODUCER
    ) as current_span:
        current_span.set_attribute("topic", topic_name)
        current_span.set_attribute("events_qty", len(events))
        try:
            timeout_settings = aiohttp.ClientTimeout(total=timeout_seconds)
            async with aiohttp.ClientSession(
                raise_for_status=True, timeout=timeout_settings
            ) as session:
                client = pubsub.PublisherClient(session=session)
                topic = client.topic_path(settings.GCP_PROJECT_ID, topic_name)
                messages = [
                    pubsub.PubsubMessage(
                        json.dumps(event.dict(), default=str).encode("utf-8")
                    )
                    for event in events
                ]
                for start in range(0, len(messages), SYSTEM_EVENTS_PUBLISH_CHUNK):
                    await client.publish(
                        topic,
                        messages[start : start + SYSTEM_EVENTS_PUBLISH_CHUNK],
                        timeout=int(timeout_settings.total),
                    )
        except Exception as e:
            logger.warning(
                f"Failed to publish {len(events)} system event(s) to {topic_name}: "
                f"{type(e).__name__}: {e}"
            )
            current_span.set_attribute("error", str(e))


async def send_event_to_integration_events_topic(event):
    """Activity logs the portal shows per integration."""
    await _publish_system_events(
        [event],
        topic_name=settings.INTEGRATION_EVENTS_TOPIC,
        timeout_seconds=settings.INTEGRATION_EVENTS_PUBLISH_TIMEOUT_SECONDS,
        span_name="routing_service.send_event_to_integration_events_topic",
    )


async def send_events_to_routing_events_topic(events):
    """Drop records (ObservationFiltered) the portal's trace consumer reads."""
    await _publish_system_events(
        events,
        topic_name=settings.ROUTING_EVENTS_TOPIC,
        timeout_seconds=settings.ROUTING_EVENTS_PUBLISH_TIMEOUT_SECONDS,
        span_name="routing_service.send_events_to_routing_events_topic",
    )


async def send_event_to_routing_events_topic(event):
    await send_events_to_routing_events_topic([event])
```

Delete the old `ROUTING_EVENTS_PUBLISH_CHUNK` constant; nothing else references it (`grep -rn ROUTING_EVENTS_PUBLISH_CHUNK app` must return nothing).

- [ ] **Step 4: Run the full suite to verify nothing moved**

Run: `TRACING_ENABLED=false .venv/bin/python -m pytest -q`
Expected: 118 passed (116 from PR #162 + Task 1 + this task). `app/tests/test_activity_logger.py::test_publish_failure_is_swallowed` and `test_source_filter_enforcement.py::test_a_failed_event_publish_never_retries_the_envelope` both still pass; they are the two swallow-on-failure pins.

- [ ] **Step 5: Commit**

```bash
uvx black==22.10.0 app/core/pubsub.py app/tests/test_source_filter_enforcement.py
git add app/core/pubsub.py app/tests/test_source_filter_enforcement.py
git commit -m "Share one best-effort publisher between the system-event topics

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: One filter partition helper for both v2 paths (#6)

**Files:**
- Modify: `app/services/event_handlers.py:256-322` (helpers), `:376-392` (single-item gate), `:629-651` (batch partition)
- Test: `app/tests/test_source_filter_enforcement.py`

**Interfaces:**
- Consumes: `_dropping_source_filter(route, destination_id, observation)` from Task 1; `_discard_filtered_observation(..., sink=...)` as in PR #162.
- Produces: `async def _partition_by_source_filter(*, route, destination, observations, current_span, span_event, set_discarded=False) -> Tuple[list, list]` returning `(allowed, filtered_events)`. `allowed` preserves input order; `filtered_events` holds one `ObservationFiltered` per drop, in input order.

- [ ] **Step 1: Write the failing test**

Append to `app/tests/test_source_filter_enforcement.py`:

```python
@pytest.mark.asyncio
async def test_single_item_drop_marks_the_span_discarded_and_batch_drop_does_not(
    mocker, enforcement_mocks, mock_gundi_client_v2, connection_v2, route_v2
):
    # The single-item span represents one observation, so a drop discards it;
    # a batch span represents the envelope, which still delivers the rest.
    _, routing_events = enforcement_mocks
    _enable(mocker)
    provider_id = str(connection_v2.provider.id)
    destination_id = str(connection_v2.destinations[0].id)
    mock_gundi_client_v2.get_route_details.return_value = async_return(
        _route_with_filter(
            route_v2, destination_id, provider_id, mode="whitelist", ids=["device-0"]
        )
    )
    # Every `with tracing.tracer.start_as_current_span(...) as span` in the
    # service now yields this one MagicMock span.
    tracer = mocker.patch("app.services.event_handlers.tracing.tracer")
    span = tracer.start_as_current_span.return_value.__enter__.return_value

    await process_observation_event(
        _observation_event_dict(provider_id, "collar-001"), _ATTRIBUTES
    )
    span.set_attribute.assert_any_call("is_discarded", True)

    span.reset_mock()
    await process_observation_event(
        _batch_event_dict(provider_id, ["device-0", "device-1"]),
        {**_ATTRIBUTES, "batch": "true", "batch_count": "2"},
    )
    assert ("is_discarded", True) not in [
        c.args for c in span.set_attribute.call_args_list
    ]
    assert routing_events.call_count == 2
    assert isinstance(routing_events.call_args_list[1][0][0], list)
```

- [ ] **Step 2: Run test to verify it fails or passes as expected**

Run: `TRACING_ENABLED=false .venv/bin/python -m pytest app/tests/test_source_filter_enforcement.py::test_single_item_drop_marks_the_span_discarded_and_batch_drop_does_not -q`
Expected: PASS before the refactor (PR #162 already sets `set_discarded=True` only on the single path). This test pins that distinction through Task 3; note "passes before refactor" and continue. If it errors on the tracer patch, fix the patch target until it passes, then continue.

- [ ] **Step 3: Add the helper and call it from both paths**

In `app/services/event_handlers.py`, directly below `_discard_filtered_observation`, add:

```python
async def _partition_by_source_filter(
    *, route, destination, observations, current_span, span_event, set_discarded=False
):
    """Split observations for one destination into (allowed, filtered_events).

    Policy runs before the destination's portal lookup and broker check on
    both v2 paths: a destination that drops everything must not be able to
    retry the envelope - and redeliver the others - by failing its setup.
    The caller publishes `filtered_events` after its deliveries so a setup
    failure does not record them twice.
    """
    allowed, filtered_events = [], []
    for observation in observations:
        rule = _dropping_source_filter(route, destination.id, observation)
        if rule is None:
            allowed.append(observation)
            continue
        await _discard_filtered_observation(
            observation=observation,
            destination=destination,
            rule=rule,
            current_span=current_span,
            span_event=span_event,
            set_discarded=set_discarded,
            sink=filtered_events,
        )
    return allowed, filtered_events
```

In `transform_and_route_observation`, replace the gate at the top of the `for destination in destinations:` loop (the `if rule := _dropping_source_filter(...)` block ending in `continue`) with:

```python
                allowed, filtered_events = await _partition_by_source_filter(
                    route=default_route,
                    destination=destination,
                    observations=[observation],
                    current_span=current_span,
                    span_event="routing_service.observation_discarded_by_source_filter",
                    set_discarded=True,
                )
                if not allowed:
                    await send_events_to_routing_events_topic(filtered_events)
                    continue
```

In `transform_and_route_observations_batch`, replace the inline partition (from `allowed, filtered_events = [], []` through the `else: allowed.append(observation)` line) with:

```python
                allowed, filtered_events = await _partition_by_source_filter(
                    route=default_route,
                    destination=destination,
                    observations=batch.observations,
                    current_span=current_span,
                    span_event="routing_service.batch_item_discarded_by_source_filter",
                )
```

Keep the `if not allowed: ... continue` that follows it and the two later `await send_events_to_routing_events_topic(filtered_events)` calls exactly where they are. Move the "Policy first, before this destination's portal lookup..." comment into the helper's docstring (done above) and delete the inline copy.

- [ ] **Step 4: Run the full suite**

Run: `TRACING_ENABLED=false .venv/bin/python -m pytest -q`
Expected: 119 passed. In particular `test_an_all_dropped_batch_skips_destination_setup`, `test_batch_drops_publish_their_events_in_one_call`, `test_a_rule_drops_only_its_own_destination` and `test_a_generic_model_destination_is_filtered_too` are the four that would catch a wrong wiring.

- [ ] **Step 5: Commit**

```bash
uvx black==22.10.0 app/services/event_handlers.py app/tests/test_source_filter_enforcement.py
git add app/services/event_handlers.py app/tests/test_source_filter_enforcement.py
git commit -m "Partition observations by routing filter in one helper for both v2 paths

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: Open the PR

**Files:** none.

- [ ] **Step 1: Push and open**

```bash
git push -u origin chore/routing-filters-followup-refactors
gh pr create --base main --title "Routing filters: follow-up refactors from the #162 review" --body "$(cat <<'EOF'
Behavior-neutral follow-ups to #162, per its review:

- `_dropping_source_filter` uses `Route.filter_for()` (gundi-core 1.14.0) instead of a `str()` dict lookup, so a non-canonical UUID spelling still matches its rule.
- One best-effort `_publish_system_events` behind `send_event_to_integration_events_topic` and `send_events_to_routing_events_topic`.
- One `_partition_by_source_filter` helper for the single-item and batch paths.

Plan: `docs/superpowers/plans/2026-10-12-routing-filters-followup-refactors.md`.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 2: Confirm CI**

Run: `gh pr checks --watch`
Expected: `run_unit_tests` and `GitGuardian Security Checks` both pass.
