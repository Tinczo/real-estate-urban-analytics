# Wgraj spark_pipeline.py do Workspace (DBFS root jest wylaczony w Free Edition)
resource "databricks_workspace_file" "spark_pipeline" {
  source = "${path.module}/../spark/src/spark_pipeline.py"
  path   = "/duze-dane/src/spark_pipeline.py"
}

resource "databricks_workspace_file" "osm_to_csv" {
  source = "${path.module}/../scripts/processing/osm_to_csv.py"
  path   = "/duze-dane/src/osm_to_csv.py"
}
