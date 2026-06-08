locals {
  base    = var.pipeline_base_path
  src     = "/Workspace/duze-dane/src/spark_pipeline.py"
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

resource "databricks_job" "pipeline_full" {
  name = "duze-dane-pipeline-full"

  environment {
    environment_key = "default"
    spec {
      client = "2"
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
    task_key        = "osm_preprocess"
    environment_key = "default"
    spark_python_task {
      python_file = "/Workspace/duze-dane/src/osm_to_csv.py"
      parameters  = [
        "--input",  "${local.base}/osm_csv",
        "--output", "${local.base}/osm_csv_out"
      ]
    }
  }

  task {
    task_key        = "e1_2"
    environment_key = "default"
    spark_python_task {
      python_file = local.src
      parameters  = [
        "--stage", "e1_2",
        "--osm_csv", "${local.base}/osm_csv_out",
        "--t1_2", local.t1_2
      ]
    }
    depends_on { task_key = "osm_preprocess" }
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

resource "databricks_job" "pipeline_full_v2" {
  name = "duze-dane-pipeline-v2"

  environment {
    environment_key = "default"
    spec {
      client = "2"
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
    task_key        = "osm_preprocess"
    environment_key = "default"
    spark_python_task {
      python_file = "/Workspace/duze-dane/src/osm_to_csv.py"
      parameters  = [
        "--input",  "${local.base}/osm_csv",
        "--output", "${local.base}/osm_csv_out"
      ]
    }
  }

  task {
    task_key        = "e1_2"
    environment_key = "default"
    spark_python_task {
      python_file = local.src
      parameters  = [
        "--stage", "e1_2",
        "--osm_csv", "${local.base}/osm_csv_out",
        "--t1_2", local.t1_2
      ]
    }
    depends_on { task_key = "osm_preprocess" }
    depends_on { task_key = "e1_5" }
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
    depends_on { task_key = "e1_5" }
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

resource "databricks_job" "experiments" {
  name = "duze-dane-experiments"

  environment {
    environment_key = "default"
    spec {
      client = "2"
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

resource "databricks_job" "experiments_sql" {
  name = "duze-dane-experiments-sql"

  environment {
    environment_key = "default"
    spec {
      client = "2"
    }
  }

  task {
    task_key        = "exp_df_e1_5"
    environment_key = "default"
    spark_python_task {
      python_file = local.src
      parameters  = [
        "--stage", "e1_5",
        "--t1_1", local.t1_1,
        "--t1_5", "${local.base}/exp_df_e1_5"
      ]
    }
  }

  task {
    task_key        = "exp_sql_e1_5"
    environment_key = "default"
    spark_python_task {
      python_file = local.src
      parameters  = [
        "--stage", "e1_5_sql",
        "--t1_1", local.t1_1,
        "--t1_5", "${local.base}/exp_sql_e1_5"
      ]
    }
  }
}

# Part 1: osm_preprocess -> e1_1 -> e1_5 -> e1_2 -> e1_3
resource "databricks_job" "pipeline_part1" {
  name = "duze-dane-pipeline-part1"

  environment {
    environment_key = "default"
    spec {
      client = "2"
    }
  }

  task {
    task_key        = "osm_preprocess"
    environment_key = "default"
    spark_python_task {
      python_file = "/Workspace/duze-dane/src/osm_to_csv.py"
      parameters  = [
        "--input",  "${local.base}/osm_csv",
        "--output", "${local.base}/osm_csv_out"
      ]
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
        "--osm_csv", "${local.base}/osm_csv_out",
        "--t1_2", local.t1_2
      ]
    }
    depends_on { task_key = "osm_preprocess" }
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
}

# Part 2: e2 -> e3
resource "databricks_job" "pipeline_part2" {
  name = "duze-dane-pipeline-part2"

  environment {
    environment_key = "default"
    spec {
      client = "2"
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
