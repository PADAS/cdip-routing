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