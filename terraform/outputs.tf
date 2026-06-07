output "pipeline_job_id" {
  description = "ID joba pełnego potoku"
  value       = databricks_job.pipeline_full.id
}

output "experiments_job_id" {
  description = "ID joba eksperymentów"
  value       = databricks_job.experiments.id
}

output "pipeline_job_url" {
  description = "URL do uruchomienia potoku w UI"
  value       = "${var.databricks_host}/#job/${databricks_job.pipeline_full.id}"
}
