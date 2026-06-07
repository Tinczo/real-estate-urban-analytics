locals {
  base    = var.pipeline_base_path
  src     = databricks_workspace_file.spark_pipeline.path
  t1_1    = "${local.base}/t1_1"
  t1_5    = "${local.base}/t1_5"
  t1_2    = "${local.base}/t1_2"
  t1_3    = "${local.base}/t1_3"
  t2      = "${local.base}/t2"
  t3      = "${local.base}/t3"
  gtfs    = "${local.base}/gtfs"
  osm_csv = "${local.base}/osm_csv"
  re_geo  = "${local.base}/real_estate_geocoded"
}

# Job uruchamiający pełny potok jako sekwencję tasków (serverless)
resource "databricks_job" "pipeline_full" {
  name = "duze-dane-pipeline-full"

  # Wymagane dla serverless Python tasks
  environment {
    environment_key = "default"
    spec {
      client = "1"
    }
  }

  task {
    task_key        = "e1_1"
    environment_key = "default"
    spark_python_task {
      python_file = local.src
      parameters  = [
        "--stage", "e1_1",
        "--stop_times", "${local.gtfs}/stop_times.txt",
        "--gtfs_cache", local.gtfs,
        "--t1_1", local.t1_1
      ]
    }
  }

  task {
    task_key        = "e1_5"
    environment_key = "default"
    spark_python_task {
      python_file = local.src
      parameters  = [
        "--stage", "e1_5",
        "--t1_1", local.t1_1,
        "--t1_5", local.t1_5
      ]
    }
    depends_on { task_key = "e1_1" }
  }

  task {
    task_key        = "e1_2"
    environment_key = "default"
    spark_python_task {
      python_file = local.src
      parameters  = [
        "--stage", "e1_2",
        "--osm_csv", local.osm_csv,
        "--t1_2", local.t1_2
      ]
    }
  }

  task {
    task_key        = "e1_3"
    environment_key = "default"
    spark_python_task {
      python_file = local.src
      parameters  = [
        "--stage", "e1_3",
        "--re_geocoded", local.re_geo,
        "--t1_3", local.t1_3
      ]
    }
  }

  task {
    task_key        = "e2"
    environment_key = "default"
    spark_python_task {
      python_file = local.src
      parameters  = [
        "--stage", "e2",
        "--t1_5", local.t1_5,
        "--t1_2", local.t1_2,
        "--t1_3", local.t1_3,
        "--t2", local.t2
      ]
    }
    depends_on { task_key = "e1_5" }
    depends_on { task_key = "e1_2" }
    depends_on { task_key = "e1_3" }
  }

  task {
    task_key        = "e3"
    environment_key = "default"
    spark_python_task {
      python_file = local.src
      parameters  = [
        "--stage", "e3",
        "--t2", local.t2,
        "--t3", local.t3
      ]
    }
    depends_on { task_key = "e2" }
  }
}

# Job do eksperymentów
resource "databricks_job" "experiments" {
  name = "duze-dane-experiments"

  environment {
    environment_key = "default"
    spec {
      client = "1"
    }
  }

  task {
    task_key        = "exp_broadcast_on"
    environment_key = "default"
    spark_python_task {
      python_file = local.src
      parameters  = [
        "--stage", "e1_1",
        "--stop_times", "${local.gtfs}/stop_times.txt",
        "--gtfs_cache", local.gtfs,
        "--t1_1", "${local.base}/exp_broadcast_on"
      ]
    }
  }

  task {
    task_key        = "exp_broadcast_off"
    environment_key = "default"
    spark_python_task {
      python_file = local.src
      parameters  = [
        "--stage", "e1_1",
        "--stop_times", "${local.gtfs}/stop_times.txt",
        "--gtfs_cache", local.gtfs,
        "--t1_1", "${local.base}/exp_broadcast_off",
        "--disable_broadcast"
      ]
    }
  }

  task {
    task_key        = "exp_partitions_1"
    environment_key = "default"
    spark_python_task {
      python_file = local.src
      parameters  = [
        "--stage", "e1_5",
        "--t1_1", local.t1_1,
        "--t1_5", "${local.base}/exp_p1",
        "--reducers", "1"
      ]
    }
  }

  task {
    task_key        = "exp_partitions_10"
    environment_key = "default"
    spark_python_task {
      python_file = local.src
      parameters  = [
        "--stage", "e1_5",
        "--t1_1", local.t1_1,
        "--t1_5", "${local.base}/exp_p10",
        "--reducers", "10"
      ]
    }
  }

  task {
    task_key        = "exp_partitions_50"
    environment_key = "default"
    spark_python_task {
      python_file = local.src
      parameters  = [
        "--stage", "e1_5",
        "--t1_1", local.t1_1,
        "--t1_5", "${local.base}/exp_p50",
        "--reducers", "50"
      ]
    }
  }
}
