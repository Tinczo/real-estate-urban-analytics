# Wgraj spark_pipeline.py do Workspace (DBFS root jest wyłączony w Free Edition)
resource "databricks_workspace_file" "spark_pipeline" {
  source = "${path.module}/../spark/src/spark_pipeline.py"
  path   = "/duze-dane/src/spark_pipeline.py"
}
