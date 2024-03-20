resource "google_secret_manager_secret" "keycloak" {
  project   = var.project_id
  secret_id = "keycloak_client_secret-routing"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "keycloak-version" {
  secret      = google_secret_manager_secret.keycloak.id
  secret_data = "change-me"
}

resource "google_secret_manager_secret_iam_member" "secret_accessor" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.keycloak.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.default.email}"
}
