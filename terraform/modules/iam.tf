resource "google_service_account" "default" {
  account_id = "routing-transformer-service"
  project    = var.project_id
}

resource "google_project_iam_member" "default" {
  project = var.project_id
  role    = "roles/run.serviceAgent"
  member  = "serviceAccount:${google_service_account.default.email}"
}

resource "google_project_iam_member" "pubsub-publisher" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.default.email}"
}

resource "google_project_iam_member" "cloudtrace-agent" {
  project = var.project_id
  role    = "roles/cloudtrace.agent"
  member  = "serviceAccount:${google_service_account.default.email}"
}