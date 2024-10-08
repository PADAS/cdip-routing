resource "google_cloud_run_v2_service" "default" {
  name     = "routing-transformer-service-${var.env}"
  project  = var.project_id
  location = var.location
  ingress  = var.ingress

  template {
    service_account = google_service_account.default.email

    scaling {
      min_instance_count = var.min_instance_count
      max_instance_count = var.max_instance_count
    }

    vpc_access {
      network_interfaces {
        network    = var.network_name
        subnetwork = var.subnet
      }
    }

    containers {
      image = var.image

      resources {
        limits            = var.limits
        cpu_idle          = true
        startup_cpu_boost = true
      }

      env {
        name  = "LOGGING_LEVEL"
        value = var.log_level
      }

      env {
        name  = "CDIP_ADMIN_ENDPOINT"
        value = var.cdip_admin_endpoint
      }

      env {
        name  = "GUNDI_API_BASE_URL"
        value = var.gundi_api_base_url
      }

      env {
        name  = "KEYCLOAK_AUDIENCE"
        value = var.keycloak_audience
      }

      env {
        name  = "KEYCLOAK_CLIENT_ID"
        value = var.keycloak_client_id
      }

      env {
        name  = "KEYCLOAK_ISSUER"
        value = var.keycloak_issuer
      }

      env {
        name  = "REDIS_HOST"
        value = var.redis_host
      }

      env {
        name  = "REDIS_PORT"
        value = var.redis_port
      }

      env {
        name  = "GCP_PROJECT_ID"
        value = var.project_id
      }

      env {
        name  = "GOOGLE_PUB_SUB_PROJECT_ID"
        value = var.project_id
      }

      env {
        name  = "DEAD_LETTER_TOPIC"
        value = google_pubsub_topic.transformer-dead-letter.name
      }

      env {
        name  = "MAX_EVENT_AGE_SECONDS"
        value = var.max_event_age_seconds
      }

      env {
        name  = "MOVEBANK_DISPATCHER_DEFAULT_TOPIC"
        value = var.movebank_dispatcher_default_topic
      }

      env {
        name  = "SMART_DEFAULT_TIMEOUT"
        value = var.smart_default_timeout
      }

      env {
        name  = "PORTAL_AUTH_TTL"
        value = var.portal_auth_ttl
      }

      env {
        name  = "TRACE_ENVIRONMENT"
        value = var.env
      }

      env {
        name = "KEYCLOAK_CLIENT_SECRET"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.keycloak.id
            version = "latest"
          }
        }
      }
    }
  }
}

