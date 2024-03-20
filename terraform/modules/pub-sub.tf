resource "google_pubsub_topic" "raw-observations" {
  name    = "raw-observations-${var.env}"
  project = var.project_id
}

resource "google_pubsub_topic" "transformer-dead-letter" {
  name    = "transformer-dead-letter-${var.env}"
  project = var.project_id
}

resource "google_eventarc_trigger" "default" {
  name            = "raw-observations-${var.env}"
  project         = var.project_id
  location        = var.location
  service_account = google_service_account.default.email

  matching_criteria {
    attribute = "type"
    value     = "google.cloud.pubsub.topic.v1.messagePublished"
  }
  destination {
    cloud_run_service {
      service = google_cloud_run_v2_service.default.name
      region  = var.location
    }
  }
  transport {
    pubsub {
      topic = google_pubsub_topic.raw-observations.id
    }
  }
}