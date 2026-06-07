# Klaster współdzielony (autoterminate po 30 min bezczynności)
resource "databricks_cluster" "pipeline_cluster" {
  cluster_name            = "duze-dane-pipeline"
  spark_version           = var.spark_version
  node_type_id            = var.cluster_node_type
  num_workers             = var.num_workers
  autotermination_minutes = 30

  spark_conf = {
    "spark.databricks.cluster.profile" = "singleNode"
    "spark.master"                     = "local[*]"
  }

  spark_env_vars = {
    "PYSPARK_PYTHON" = "/databricks/python3/bin/python3"
  }

  # Free Edition nie wspiera policyId — usuń jeśli nie masz polityki
  # policy_id = databricks_cluster_policy.pipeline_policy.id
}
