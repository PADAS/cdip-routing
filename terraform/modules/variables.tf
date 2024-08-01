variable "project_id" {
  type        = string
  description = "The GCP project id"
}

variable "env" {
  type        = string
  description = "The environment name"
}

variable "location" {
  type        = string
  description = "The location of the cloud run service"
  default     = "us-central1"
}

variable "ingress" {
  type        = string
  description = "Provides the ingress settings for this Service"
  default     = "INGRESS_TRAFFIC_INTERNAL_ONLY"
}

variable "min_instance_count" {
  type        = number
  description = "Minimum number of serving instances that this resource should have"
  default     = 0
}

variable "max_instance_count" {
  type        = number
  description = "Maximum number of serving instances that this resource should have."
  default     = "2"
}

variable "image" {
  description = "GCR hosted image URL to deploy"
  type        = string
}

# template spec container
# resources
# cpu = (core count * 1000)m
# memory = (size) in Mi/Gi
variable "limits" {
  type        = map(string)
  description = "Resource limits to the container"
  default     = { "cpu" = "1000m", "memory" = "512Mi" }
}

variable "log_level" {
  type        = string
  description = "The Log level used in the cloud run app"
  default     = "INFO"
}

variable "cdip_admin_endpoint" {
  type = string
}

variable "gundi_api_base_url" {
  type = string
}

variable "keycloak_audience" {
  type    = string
  default = "cdip-admin-portal"
}

variable "keycloak_client_id" {
  type    = string
  default = "cdip-routing"
}

variable "keycloak_issuer" {
  type = string
}

variable "redis_host" {
  type = string
}

variable "redis_port" {
  type    = string
  default = "6379"
}

variable "max_event_age_seconds" {
  type    = string
  default = "300"
}

variable "movebank_dispatcher_default_topic" {
  type = string
}

variable "smart_default_timeout" {
  type    = string
  default = "300"
}

variable "portal_auth_ttl" {
  type    = string
  default = "300"
}

variable "network_name" {
  type        = string
  description = "VPC network name"
}

variable "subnet" {
  type        = string
  default     = "cloud-run"
  description = "The subnet cloud run will use"
}