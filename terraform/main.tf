terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.40"
    }
  }
  required_version = ">= 1.3"
}

provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}
