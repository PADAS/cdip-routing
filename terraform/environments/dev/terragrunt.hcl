include "root" {
  path = find_in_parent_folders()
}

terraform {
  source = "../../modules"
}

inputs = {
  image = "" # Override at pipeline level

  project_id                        = "cdip-dev-78ca"
  env                               = "dev"
  cdip_admin_endpoint               = "https://api.dev.gundiservice.org"
  gundi_api_base_url                = "https://api.dev.gundiservice.org"
  keycloak_issuer                   = "https://cdip-auth.pamdas.org/auth/realms/cdip-dev"
  redis_host                        = "10.3.176.132"
  movebank_dispatcher_default_topic = "destination-movebank-dev"
}