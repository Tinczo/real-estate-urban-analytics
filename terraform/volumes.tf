resource "databricks_schema" "duze_dane" {
  catalog_name = "workspace"
  name         = "duze_dane"
}

resource "databricks_volume" "data" {
  catalog_name = "workspace"
  schema_name  = databricks_schema.duze_dane.name
  name         = "data"
  volume_type  = "MANAGED"
}

resource "databricks_grants" "data_volume" {
  volume = "${databricks_schema.duze_dane.catalog_name}.${databricks_schema.duze_dane.name}.${databricks_volume.data.name}"

  grant {
    principal  = "account users"
    privileges = ["ALL PRIVILEGES"]
  }
}
