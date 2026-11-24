variable "project_id" {
  type        = string
  description = "GCP project hosting the data platform."
}

variable "region" {
  type    = string
  default = "europe-west1"
}

variable "location" {
  type        = string
  default     = "EU"
  description = "BigQuery / GCS multi-region."
}

variable "env" {
  type        = string
  default     = "dev"
  description = "Environment suffix (dev | prod)."
  validation {
    condition     = contains(["dev", "prod"], var.env)
    error_message = "env must be dev or prod."
  }
}

variable "raw_retention_days" {
  type    = number
  default = 90
}

variable "backup_schedule" {
  type        = string
  default     = "every sunday 03:00"
  description = "Data Transfer Service schedule for the weekly marts backup."
}

variable "composer_service_account" {
  type        = string
  default     = null
  description = "Optional: the Composer environment's SA, allowed to act as the pipeline SA."
}
