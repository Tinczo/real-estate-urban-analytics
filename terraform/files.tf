# Wgraj spark_pipeline.py do DBFS
resource "databricks_dbfs_file" "spark_pipeline" {
  source = "${path.module}/../spark/src/spark_pipeline.py"
  path   = "/FileStore/duze-dane/src/spark_pipeline.py"
}
