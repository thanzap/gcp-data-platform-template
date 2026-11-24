# Feed these into the Composer environment as Airflow Variables.
output "airflow_variables" {
  value = {
    raw_bucket               = google_storage_bucket.raw.name
    staging_dataset          = "${var.project_id}.${google_bigquery_dataset.staging.dataset_id}"
    revenue_table            = "${var.project_id}.${google_bigquery_dataset.marts.dataset_id}.${google_bigquery_table.revenue_daily.table_id}"
    example_ssp_token_secret = "${google_secret_manager_secret.example_ssp_token.id}/versions/latest"
  }
}

output "pipelines_service_account" {
  value = google_service_account.pipelines.email
}
