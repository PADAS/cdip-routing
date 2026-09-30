resource "google_pubsub_topic" "raw-observations" {
  name    = "raw-observations-${var.env}"
  project = var.project_id
}

resource "google_pubsub_topic" "transformer-dead-letter" {
  name    = "transformer-dead-letter-${var.env}"
  project = var.project_id
}

resource "google_pubsub_subscription" "transformer-subscription" {
  name    = "raw-observations-transformer-${var.env}"
  topic   = google_pubsub_topic.raw-observations.id
  project = var.project_id

  ack_deadline_seconds    = 600
  enable_message_ordering = true

  expiration_policy {
    ttl = ""
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  push_config {
    push_endpoint = google_cloud_run_v2_service.default.uri

    oidc_token {
      service_account_email = google_service_account.default.email
    }
  }
}

resource "google_pubsub_subscription" "transformer-dead-letter-subscription" {
  name    = "transformer-dead-letter-subscription-${var.env}"
  topic   = google_pubsub_topic.transformer-dead-letter.id
  project = var.project_id

  ack_deadline_seconds    = 60
  enable_message_ordering = false

  expiration_policy {
    ttl = ""
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  message_retention_duration = "604800s" # 7 days in seconds
}

# Routing domain events (first: ObservationFiltered), consumed by the portal's
# routing-events consumer. The consumer must be deployed before anything
# publishes here: the pipeline is forward-only, so events acked with no
# consumer are lost. See GUNDI-5711.
resource "google_pubsub_topic" "routing-events" {
  name    = "routing-events-${var.env}"
  project = var.project_id
}

resource "google_pubsub_subscription" "routing-events-portal-subscription" {
  name    = "cdip-routing-events-sub-${var.env}"
  topic   = google_pubsub_topic.routing-events.id
  project = var.project_id

  ack_deadline_seconds    = 60
  enable_message_ordering = true

  expiration_policy {
    ttl = ""
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  message_retention_duration = "604800s" # 7 days in seconds
}