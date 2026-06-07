locals {
  base      = var.pipeline_base_path
  src       = "/FileStore/duze-dane/src/spark_pipeline.py"
  t1_1      = "${local.base}/t1_1"
  t1_5      = "${local.base}/t1_5"
  t1_2      = "${local.base}/t1_2"
  t1_3      = "${local.base}/t1_3"
  t2        = "${local.base}/t2"
  t3        = "${local.base}/t3"
  gtfs      = "${local.base}/gtfs"
  osm_csv   = "${local.base}/osm_csv"
  re_geo    = "${local.base}/real_estate_geocoded"
}

# Job uruchamiający pełny potok jako sekwencję tasków
resource "databricks_job" "pipeline_full" {
  name = "duze-dane-pipeline-full"

  # Współdzielony klaster (job_cluster zamiast existing_cluster_id = droższy Free tier)
  job_cluster {
    job_cluster_key = "pipeline"
    new_cluster {
      spark_version  = var.spark_version
      node_type_id   = var.cluster_node_type
      num_workers    = var.num_workers
    }
  }

  # E1.1 — GTFS extract + broadcast join
  task {
    task_key        = "e1_1"
    job_cluster_key = "pipeline"
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

  # E1.5 — agregacja GTFS
  task {
    task_key        = "e1_5"
    job_cluster_key = "pipeline"
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

  # E1.2 — OSM CSV extract
  task {
    task_key        = "e1_2"
    job_cluster_key = "pipeline"
    spark_python_task {
      python_file = local.src
      parameters  = [
        "--stage", "e1_2",
        "--osm_csv", local.osm_csv,
        "--t1_2", local.t1_2
      ]
    }
  }

  # E1.3 — normalizacja nieruchomości
  task {
    task_key        = "e1_3"
    job_cluster_key = "pipeline"
    spark_python_task {
      python_file = local.src
      parameters  = [
        "--stage", "e1_3",
        "--re_geocoded", local.re_geo,
        "--t1_3", local.t1_3
      ]
    }
  }

  # E2 — wskaźniki per nieruchomość (czeka na E1.5, E1.2, E1.3)
  task {
    task_key        = "e2"
    job_cluster_key = "pipeline"
    spark_python_task {
      python_file = local.src
      parameters  = [
        "--stage", "e2",
        "--t1_5", local.t1_5,
        "--t1_2", local.t1_2,
        "--t1_3", local.t1_3,
        "--t2",   local.t2
      ]
    }
    depends_on { task_key = "e1_5" }
    depends_on { task_key = "e1_2" }
    depends_on { task_key = "e1_3" }
  }

  # E3 — korelacja Pearsona
  task {
    task_key        = "e3"
    job_cluster_key = "pipeline"
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

# Job do eksperymentów (broadcast vs sort-merge, liczba partycji)
resource "databricks_job" "experiments" {
  name = "duze-dane-experiments"

  job_cluster {
    job_cluster_key = "exp"
    new_cluster {
      spark_version = var.spark_version
      node_type_id  = var.cluster_node_type
      num_workers   = var.num_workers
    }
  }

  # Eksperyment A: Broadcast ON
  task {
    task_key        = "exp_broadcast_on"
    job_cluster_key = "exp"
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

  # Eksperyment A: Broadcast OFF
  task {
    task_key        = "exp_broadcast_off"
    job_cluster_key = "exp"
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

  # Eksperyment B: partycje = 1
  task {
    task_key        = "exp_partitions_1"
    job_cluster_key = "exp"
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

  # Eksperyment B: partycje = 10
  task {
    task_key        = "exp_partitions_10"
    job_cluster_key = "exp"
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

  # Eksperyment B: partycje = 50
  task {
    task_key        = "exp_partitions_50"
    job_cluster_key = "exp"
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
