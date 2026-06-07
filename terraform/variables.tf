variable "databricks_host" {
  description = "URL workspace Databricks, np. https://adb-xxx.azuredatabricks.net"
  type        = string
}

variable "databricks_token" {
  description = "Personal Access Token z Databricks (Settings > Developer > Access tokens)"
  type        = string
  sensitive   = true
}

variable "cluster_node_type" {
  description = "Typ węzła klastra (Free Edition: Standard_DS3_v2 / i3.xlarge)"
  type        = string
  default     = "Standard_DS3_v2"
}

variable "spark_version" {
  description = "Wersja Spark/DBR"
  type        = string
  default     = "13.3.x-scala2.12"
}

variable "num_workers" {
  description = "Liczba worker nodes"
  type        = number
  default     = 2
}

variable "pipeline_base_path" {
  description = "Ścieżka bazowa w DBFS dla danych wejściowych"
  type        = string
  default     = "/FileStore/duze-dane"
}
