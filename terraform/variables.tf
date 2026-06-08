variable "databricks_host" {
  description = "URL workspace Databricks, np. https://adb-xxx.azuredatabricks.net"
  type        = string
}

variable "databricks_token" {
  description = "Personal Access Token z Databricks (Settings > Developer > Access tokens)"
  type        = string
  sensitive   = true
}

variable "pipeline_base_path" {
  description = "Ścieżka bazowa w Unity Catalog Volume"
  type        = string
  default     = "/Volumes/workspace/duze_dane/data"
}
