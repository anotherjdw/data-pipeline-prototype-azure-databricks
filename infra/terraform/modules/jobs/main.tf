terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "1.136.0"
    }
  }
}

locals {
  common_tags = {
    project     = var.project
    environment = var.environment
    managed_by  = "terraform"
    module      = "jobs"
  }
}

resource "databricks_job" "etl_jobs" {
  for_each = var.jobs

  name                = "${var.project}-${var.environment}-${each.key}"
  max_concurrent_runs = 2
  tags                = local.common_tags

  task {
    task_key    = each.key
    max_retries = 0

    # A job cluster: created for each run and removed afterwards (like a Glue job's workers).
    new_cluster {
      spark_version = var.spark_version
      node_type_id  = each.value.cluster.node_type_id
      num_workers   = each.value.cluster.num_workers
      # Unity Catalog needs an access mode. Standard (shared) mode works for this job, since
      # no code uses spark._jsc / spark._jvm.
      data_security_mode = "USER_ISOLATION"

      # num_workers = 0 makes a single-node cluster: the driver runs Spark locally. Databricks
      # needs the singleNode profile, a local master and the SingleNode tag to accept it.
      custom_tags = merge(
        local.common_tags,
        each.value.cluster.num_workers == 0 ? { "ResourceClass" = "SingleNode" } : {}
      )

      spark_conf = merge(
        {
          # transform derives date_utc with to_date(), which uses the session time zone.
          "spark.sql.session.timeZone" = "UTC"
        },
        each.value.cluster.num_workers == 0 ? {
          "spark.databricks.cluster.profile" = "singleNode"
          "spark.master"                     = "local[*]"
        } : {}
      )
    }

    # Runs the job's entry script; the deploy workflow uploads it as <volume>/jobs/<job>/job.py.
    # Each YAML argument KEY: value becomes --KEY value on the command line argparse reads.
    spark_python_task {
      python_file = "${var.code_volume_path}/jobs/${each.key}/job.py"
      parameters  = flatten([for arg, value in each.value.arguments : ["--${arg}", value]])
    }

    # The data_pipeline_prototype package (transformations, utils, tables) the entry script
    # imports. Replaces Glue's --extra-py-files zip.
    library {
      whl = "${var.code_volume_path}/${var.wheel_file_name}"
    }
  }

  # Only jobs listed in scheduled_triggers get a schedule.
  dynamic "schedule" {
    for_each = contains(keys(var.scheduled_triggers), each.key) ? [var.scheduled_triggers[each.key]] : []
    content {
      quartz_cron_expression = schedule.value
      timezone_id            = "UTC"
      pause_status           = var.schedules_paused ? "PAUSED" : "UNPAUSED"
    }
  }
}
