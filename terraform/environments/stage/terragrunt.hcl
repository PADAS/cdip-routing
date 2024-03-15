include "root" {
  path = find_in_parent_folders()
}

terraform {
  source = "../../modules"
}

inputs = {
  image = "" # Override at pipeline level

  project_id                        = "cdip-stage-78ca"
  env                               = "stage"
  cdip_admin_endpoint               = "https://api.stage.gundiservice.org"
  gundi_api_base_url                = "https://api.stage.gundiservice.org"
  keycloak_issuer                   = "https://cdip-auth.pamdas.org/auth/realms/cdip-dev"
  redis_host                        = "10.243.180.36"
  movebank_dispatcher_default_topic = "destination-movebank-stage"
}