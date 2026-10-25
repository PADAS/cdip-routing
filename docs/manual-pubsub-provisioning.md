# Manual Pub/Sub provisioning for new destination action runners

When a new destination integration is added to Gundi that runs on a **single-service-per-type** action runner (the same pattern as InReach and CMORE — one Cloud Run service handles all customer integrations of that type), cdip-routing needs a dedicated Pub/Sub topic to publish to, and the runner needs a push subscription on that topic.

The portal/Terraform flow auto-provisions the *actions* side (`<integration>-actions-topic`, `<integration>-actions-sub` → Cloud Run root) during integration type registration. It does **not** auto-provision the *push-data* side. Until [GUNDI-5364](https://allenai.atlassian.net/browse/GUNDI-5364) automates this, the four resources below must be created by hand for each environment (dev, stage, prod).

## Resources to create

For an integration type with slug `<type>` in GCP project `<project>`, with the runner exposed at `<runner-url>`:

| Resource | Kind | Notes |
|---|---|---|
| `<type>-push-data-topic` | topic | cdip-routing publishes `GundiDelivery` (or legacy destination-specific payloads) here |
| `<type>-push-data-dead-letter` | topic | dead-letter target for the push subscription |
| `<type>-push-data-dead-letter-sub` | subscription | retains undelivered messages; no push endpoint |
| `<type>-push-data-sub` | subscription | push to `<runner-url>/push-data` with OIDC auth |

## Commands

Worked example: provisioning CMORE in `cdip-stage-78ca` with runner URL `https://cmore-actions-runner-jabcutl7za-uc.a.run.app`. Substitute project, type slug, and runner URL for your case.

```bash
PROJECT=cdip-stage-78ca
TYPE=cmore
RUNNER_URL=https://cmore-actions-runner-jabcutl7za-uc.a.run.app
RUNNER_SA=gundi-integrations-v2@${PROJECT}.iam.gserviceaccount.com

# 1. Push-data topic (where cdip-routing publishes destination payloads)
gcloud pubsub topics create ${TYPE}-push-data-topic \
  --project=${PROJECT}

# 2. Dead-letter topic for the push-data subscription
gcloud pubsub topics create ${TYPE}-push-data-dead-letter \
  --project=${PROJECT}

# 3. Dead-letter subscription (no push endpoint; just retains undelivered messages)
gcloud pubsub subscriptions create ${TYPE}-push-data-dead-letter-sub \
  --project=${PROJECT} \
  --topic=${TYPE}-push-data-dead-letter \
  --ack-deadline=600 \
  --message-retention-duration=7d

# 4. Push subscription routing to the action runner's /push-data endpoint
gcloud pubsub subscriptions create ${TYPE}-push-data-sub \
  --project=${PROJECT} \
  --topic=${TYPE}-push-data-topic \
  --push-endpoint=${RUNNER_URL}/push-data \
  --push-auth-service-account=${RUNNER_SA} \
  --ack-deadline=600 \
  --message-retention-duration=7d \
  --min-retry-delay=10s \
  --max-retry-delay=600s \
  --dead-letter-topic=${TYPE}-push-data-dead-letter \
  --max-delivery-attempts=5 \
  --expiration-period=2678400s
```

## After provisioning

1. **Set `broker_config.topic` on each destination integration in the Gundi portal** to `<type>-push-data-topic`. Without this, `cdip-routing` falls back to its legacy naming convention `destination-<id>-<GCP_ENVIRONMENT>` (see `app/core/pubsub.py`) and messages will never reach the runner.

2. **Grant the Pub/Sub service agent `roles/iam.serviceAccountTokenCreator`** on the runner SA, if this hasn't already been set up in the environment. Without it, the push subscription's OIDC token-minting will fail and the runner will reject deliveries with 401:

   ```bash
   PROJECT_NUMBER=$(gcloud projects describe ${PROJECT} --format='value(projectNumber)')
   gcloud iam service-accounts add-iam-policy-binding ${RUNNER_SA} \
     --project=${PROJECT} \
     --member=serviceAccount:service-${PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com \
     --role=roles/iam.serviceAccountTokenCreator
   ```

3. **Verify** by publishing a test message and tailing the runner logs:

   ```bash
   gcloud pubsub topics publish ${TYPE}-push-data-topic \
     --project=${PROJECT} \
     --message='{"data": "test"}'

   gcloud run services logs read cmore-actions-runner \
     --project=${PROJECT} --region=us-central1 --limit=20
   ```

## Why this is manual

The Gundi portal's integration-type registration flow provisions the *actions* topic (used for portal-triggered pull actions) but does not yet know about the *push-data* topic (used when cdip-routing forwards destination traffic). [GUNDI-5364](https://allenai.atlassian.net/browse/GUNDI-5364) tracks moving this into the same Terraform/registration path.
