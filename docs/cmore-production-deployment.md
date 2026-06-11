# CMORE integration — production deployment runbook

End-to-end production rollout of the EarthRanger → Gundi → CMORE integration. Covers four code deploys plus the non-code pieces (Pub/Sub provisioning, portal config, the external CMORE tag-visibility dependency) that are easy to miss.

This was built and verified on dev/stage; the same chain applies to prod. There is **no database migration** and **no data backfill** in this change set — `provider_metadata` is a write-only pass-through serializer field, not persisted.

> ⚠️ **Start the long pole first (Step 0).** The prod CMORE ShareGroup must be subscribed to the relevant tag domain(s) by a CMORE/CHPC admin. The API cannot do this; it's portal-admin only and was the original dev blocker. Kick this off before anything else.

---

## Parameters (set once, used throughout)

```bash
# --- confirm these for prod before running anything ---
export PROJECT=cdip-prod-78ca               # CONFIRM (dev=cdip-dev-78ca, stage=cdip-stage-78ca)
export REGION=us-central1
export TYPE=cmore
export RUNNER_URL=https://<cmore-actions-runner-prod-url>   # from `gcloud run services describe`
export RUNNER_SA=gundi-integrations-v2@${PROJECT}.iam.gserviceaccount.com
```

---

## What's shipping

| Service | Change | Dependency pin to verify in the prod image |
|---|---|---|
| **cdip** (Sensors API) | accept + wire `provider_metadata` through to routing | `gundi-core==1.12.0` |
| **cdip-routing** | preserve `provider_metadata` during deserialization | `gundi-core==1.12.0` |
| **gundi-integration-cmore** | type-aware tag values, deep-link comment, event-update→comment, gundi_id correlation key | `gundi-client-v2~=3.3.0` |
| **gundi-integration-earthranger** | event-type/subject-group filters, event updates, `source=event_type`, `er_ui_root` deep link | erclient, gundi-core |

Libraries `gundi-core 1.12.0` and `gundi-client-v2 3.3.0` are already published to PyPI; the merged requirements pins bake them into the images, so deploying the images is sufficient. No separate library release step.

---

## Step 0 — External prerequisite (kick off first)

- [ ] **CMORE ShareGroup tag visibility.** Ask the CMORE/CHPC admin to subscribe the **prod** ShareGroup (used by the prod CMORE integration token) to the Wildlife tag domain (and any others in scope). Verify after with `get-tags`:
  ```bash
  CMORE_BASE_URL=<prod-cmore-base-url> CMORE_TOKEN=<prod-token> \
    python -m app.datasource.cli --base-url "$CMORE_BASE_URL" --token "$CMORE_TOKEN" get-tags
  ```
  Expect the Wildlife domain with its tags. Zero tags = not subscribed yet → tag attachment will silently no-op.

---

## Step 1 — Deploy the upstream services (order matters)

Deploy these **before** the runners so `provider_metadata` survives the chain (stamping a field that a downstream drops is harmless, but get the persisters in first).

- [ ] **cdip (Sensors API).** Deploy prod. Verify the image is from a commit that includes PR #430 and pins `gundi-core==1.12.0`. Confirm `provider_metadata` appears on `POST /v2/events/` in the prod OpenAPI spec. No migration to run.
- [ ] **cdip-routing.** Deploy prod (PR #149 — `gundi-core==1.12.0`). Verify the deployed image SHA matches the merge commit:
  ```bash
  gcloud run services describe routing-transformer-service-prod \
    --project=${PROJECT} --region=${REGION} \
    --format='value(spec.template.spec.containers[0].image)'
  ```

## Step 2 — Provision prod Pub/Sub for CMORE

The portal/Terraform flow provisions the *actions* topic but **not** the *push-data* topic (GUNDI-5364). Create these by hand (adapted from `docs/manual-pubsub-provisioning.md`):

```bash
# 1. push-data topic (cdip-routing publishes GundiDelivery here)
gcloud pubsub topics create ${TYPE}-push-data-topic --project=${PROJECT}

# 2. dead-letter topic
gcloud pubsub topics create ${TYPE}-push-data-dead-letter --project=${PROJECT}

# 3. dead-letter subscription (retain only)
gcloud pubsub subscriptions create ${TYPE}-push-data-dead-letter-sub \
  --project=${PROJECT} --topic=${TYPE}-push-data-dead-letter \
  --ack-deadline=600 --message-retention-duration=7d

# 4. push subscription → runner /push-data with OIDC
gcloud pubsub subscriptions create ${TYPE}-push-data-sub \
  --project=${PROJECT} --topic=${TYPE}-push-data-topic \
  --push-endpoint=${RUNNER_URL}/push-data \
  --push-auth-service-account=${RUNNER_SA} \
  --ack-deadline=600 --message-retention-duration=7d \
  --min-retry-delay=10s --max-retry-delay=600s \
  --dead-letter-topic=${TYPE}-push-data-dead-letter \
  --max-delivery-attempts=5 --expiration-period=2678400s
```

- [ ] Grant the Pub/Sub service agent `roles/iam.serviceAccountTokenCreator` on the runner SA (else OIDC push → 401):
  ```bash
  PROJECT_NUMBER=$(gcloud projects describe ${PROJECT} --format='value(projectNumber)')
  gcloud iam service-accounts add-iam-policy-binding ${RUNNER_SA} \
    --project=${PROJECT} \
    --member=serviceAccount:service-${PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com \
    --role=roles/iam.serviceAccountTokenCreator
  ```

> The push-data topic must exist **before** the CMORE destination integration is pointed at it and before routing tries to deliver.

## Step 3 — Deploy the runners

- [ ] **gundi-integration-earthranger** — deploy prod. Confirm `IntegrationType` self-registration succeeds (`REGISTER_ON_START`); slug is `earth_ranger` (rename is GUNDI-5385, deferred).
- [ ] **gundi-integration-cmore** — deploy prod. Confirm self-registration. Image must pin `gundi-client-v2~=3.3.0`.
- [ ] Per-runner prod env/secrets: CMORE token, ER token, Redis host/port, `KEYCLOAK_CLIENT_SECRET`, `GUNDI_API_BASE_URL` (→ prod), `INTEGRATION_EVENTS_TOPIC`, topic names.

## Step 4 — Configure prod integrations in the Gundi portal

- [ ] **CMORE destination integration**: prod base_url + token + ShareGroup. Set `broker_config.topic = cmore-push-data-topic` (else cdip-routing falls back to legacy `destination-<id>-<env>` naming and messages never arrive). No `additional.generic_model` flag needed — cdip-routing routes the generic-model path by destination *type* (`cmore` is in `GENERIC_MODEL_DESTINATION_TYPES`, default; cdip-routing PR #151). The per-integration flag still works as an override if ever needed.
- [ ] **ER provider integration**: pull filters (event types + subject groups) and `er_ui_root` (for the deep link).
- [ ] **Connection**: provider = ER, destination = CMORE.
- [ ] **DeliverConfig mappings** (`event_type_to_tag`): run the scaffold CLI against the prod connection once tags are visible (Step 0):
  ```bash
  python -m app.datasource.cli scaffold-mapping \
    --gundi-username <you> --connection <prod-connection-id> \
    --event-type <type> --write
  ```

---

## Verification (reuse the dev/stage acceptance checks)

1. **Deep link** — create an ER event → the CMORE event shows a `Source: <er-url>` comment.
2. **Tag resolution** — a mapped event type (e.g. `rhino_carcass`) → the CMORE lookup fields populate with resolved values (not raw ER slugs). Watch for `Attached CMORE tag ... with N field value(s)` and any `has no option matching` warnings.
3. **Two-event update correlation** (the gundi_id-key fix) — create **two** events of the same type (A then B), edit **A**, and confirm the comment lands on **A**, not B. In the CMORE runner logs, the `Posted ... comment (root_message_id=...)` line must reference **A's** messageId.

Tail logs during verification:
```bash
gcloud run services logs read cmore-actions-runner --project=${PROJECT} --region=${REGION} --limit=100
```

---

## Go / No-Go

**Go** when: upstream services deployed on `gundi-core 1.12.0`; push-data topic + subscription live with token-creator IAM; both runners registered; portal connection + mappings in place; CMORE ShareGroup has tag visibility; all three verification checks pass.

**No-Go / hold** if: `get-tags` returns zero tags (Step 0 incomplete — events post but tags silently drop); the push subscription 401s (missing token-creator role); or deep link arrives `None` at CMORE (an upstream service is not on 1.12.0).

## Rollback

No schema/data changes, so rollback is image-level and low-risk:
- Redeploy the prior image for any service.
- `provider_metadata` degrades gracefully: an older cdip/cdip-routing simply drops the field (deep-link comment stops; events still post). An older CMORE runner ignores tags/updates it doesn't understand.
- Pub/Sub resources can be left in place (idle) or deleted; they don't affect other types.
- To fall a destination back to legacy routing, remove its `broker_config.topic` (and, if cdip-routing PR #151 isn't deployed, the generic-model path is instead controlled by `GENERIC_MODEL_DESTINATION_TYPES` / the `additional.generic_model` override).
