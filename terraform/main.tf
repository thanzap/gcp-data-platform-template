locals {
  labels = { env = var.env, managed_by = "terraform", system = "data-platform" }
}

# ---------------------------------------------------------------- storage ----
# Raw API output. Kept so any day can be reloaded without calling the API again,
# then deleted after var.raw_retention_days.
resource "google_storage_bucket" "raw" {
  name                        = "${var.project_id}-raw-${var.env}"
  location                    = var.location
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  labels                      = local.labels

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = var.raw_retention_days
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age        = 7
      with_state = "ARCHIVED"
    }
    action {
      type = "Delete"
    }
  }
}

# --------------------------------------------------------------- bigquery ----
resource "google_bigquery_dataset" "staging" {
  dataset_id                  = "staging_${var.env}"
  location                    = var.location
  description                 = "Per-run load tables; expire automatically."
  default_table_expiration_ms = 2 * 24 * 60 * 60 * 1000
  labels                      = local.labels
}

resource "google_bigquery_dataset" "marts" {
  dataset_id  = "marts_${var.env}"
  location    = var.location
  description = "Cleaned, query-ready tables."
  labels      = local.labels
}

resource "google_bigquery_dataset" "backup" {
  dataset_id                  = "marts_${var.env}_backup"
  location                    = var.location
  description                 = "Weekly copy of marts (Data Transfer Service). 30-day table expiry."
  default_table_expiration_ms = 30 * 24 * 60 * 60 * 1000
  labels                      = local.labels
}

resource "google_bigquery_table" "revenue_daily" {
  dataset_id          = google_bigquery_dataset.marts.dataset_id
  table_id            = "revenue_daily"
  description         = "Daily revenue per source, site, ad unit and country."
  schema              = file("${path.module}/../schemas/revenue_daily.json")
  deletion_protection = var.env == "prod"
  labels              = local.labels

  time_partitioning {
    type  = "DAY"
    field = "date"
  }
  require_partition_filter = true # every query must prune: no accidental full scans
  clustering               = ["source", "site_domain"]
}

# ---------------------------------------------------------- identity/IAM ----
# One least-privilege SA for pipelines: dataset-level roles, not project-wide.
resource "google_service_account" "pipelines" {
  account_id   = "data-pipelines-${var.env}"
  display_name = "Data pipelines (${var.env})"
}

resource "google_project_iam_member" "pipelines_jobs" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipelines.email}"
}

resource "google_bigquery_dataset_iam_member" "pipelines_write" {
  for_each   = toset([google_bigquery_dataset.staging.dataset_id, google_bigquery_dataset.marts.dataset_id])
  dataset_id = each.value
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipelines.email}"
}

resource "google_storage_bucket_iam_member" "pipelines_raw" {
  bucket = google_storage_bucket.raw.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipelines.email}"
}

resource "google_service_account_iam_member" "composer_can_use_pipelines_sa" {
  count              = var.composer_service_account == null ? 0 : 1
  service_account_id = google_service_account.pipelines.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${var.composer_service_account}"
}

# ---------------------------------------------------------------- secrets ----
# The secret container only; the token value is added out-of-band
# (gcloud secrets versions add), so it never lands in Terraform state.
resource "google_secret_manager_secret" "example_ssp_token" {
  secret_id = "example-ssp-token-${var.env}"
  labels    = local.labels
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_iam_member" "pipelines_read_token" {
  secret_id = google_secret_manager_secret.example_ssp_token.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.pipelines.email}"
}

# ---------------------------------------------------------------- backups ----
# Weekly copy of the marts dataset. Restore = copy tables back.
resource "google_service_account" "backup" {
  account_id   = "bq-backup-${var.env}"
  display_name = "BigQuery weekly backup (${var.env})"
}

resource "google_bigquery_dataset_iam_member" "backup_read" {
  dataset_id = google_bigquery_dataset.marts.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.backup.email}"
}

resource "google_bigquery_dataset_iam_member" "backup_write" {
  dataset_id = google_bigquery_dataset.backup.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.backup.email}"
}

resource "google_project_iam_member" "backup_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin" # required by DTS dataset copy to create tables in the destination
  member  = "serviceAccount:${google_service_account.backup.email}"
}

resource "google_bigquery_data_transfer_config" "weekly_backup" {
  display_name           = "Weekly marts backup (${var.env})"
  location               = var.location
  data_source_id         = "cross_region_copy"
  schedule               = var.backup_schedule
  destination_dataset_id = google_bigquery_dataset.backup.dataset_id
  service_account_name   = google_service_account.backup.email
  params = {
    source_project_id           = var.project_id
    source_dataset_id           = google_bigquery_dataset.marts.dataset_id
    overwrite_destination_table = "true"
  }
  depends_on = [
    google_bigquery_dataset_iam_member.backup_read,
    google_bigquery_dataset_iam_member.backup_write,
    google_project_iam_member.backup_admin,
  ]
}
