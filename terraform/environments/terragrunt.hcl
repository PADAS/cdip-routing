remote_state {
  backend = "gcs"
  config = {
    bucket   = "gundi-tf-state"
    prefix   = "cdip-routing/${path_relative_to_include()}/terraform.tfstate"
    project  = "cdip-78ca"
    location = "us"
  }
}