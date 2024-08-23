include "root" {
  path = find_in_parent_folders()
}

terraform {
  source = "../../modules"
}

inputs = {
  image = "" # Override at pipeline level

  project_id                        = "cdip-prod1-78ca"
  env                               = "prod"
  cdip_admin_endpoint               = "https://api.gundiservice.org"
  gundi_api_base_url                = "https://api.gundiservice.org"
  keycloak_issuer                   = "https://cdip-auth.pamdas.org/auth/realms/cdip-prod"
  redis_host                        = "10.208.106.172"
  movebank_dispatcher_default_topic = "destination-movebank-prod"
<<<<<<< HEAD
  max_event_age_seconds             = "86400"
  network_name                      = "sintegrate-vpc-4bb07e9c"
=======
  max_event_age_seconds             = "120"
>>>>>>> hotfix-smart-errors
  max_instance_count                = 20
}