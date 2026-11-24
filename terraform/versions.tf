terraform {
  required_version = ">= 1.6"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
  # Remote state: create the bucket once, then uncomment.
  # backend "gcs" {
  #   bucket = "my-project-tfstate"
  #   prefix = "data-platform"
  # }
}

provider "google" {
  project = var.project_id
  region  = var.region
}
