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

  # Fails a run that takes longer, including one stuck waiting for a cluster
  timeout_seconds = var.timeout_seconds

  # Failed or timed out runs send an e-mail
  dynamic "email_notifications" {
    for_each = length(var.notification_emails) > 0 ? [var.notification_emails] : []
    content {
      on_failure                = email_notifications.value
      no_alert_for_skipped_runs = true
    }
  }

  task {
    task_key    = each.key
    max_retries = 0

    # A job cluster: created for each run and removed afterwards (like a Glue job's workers).
    new_cluster {
      spark_version = var.spark_version
      node_type_id  = each.value.cluster.node_type_id
      num_workers   = each.value.cluster.num_workers
      # Unity Catalog needs an access mode. Dedicated mode, assigned to the identity the job runs
      # as: in Standard mode the isolated Python sandbox could not read job.py from the volume.
      data_security_mode = "SINGLE_USER"
      single_user_name   = var.single_user_name
      custom_tags        = local.common_tags

      spark_conf = {
        # transform derives date_utc with to_date(), which uses the session time zone.
        "spark.sql.session.timeZone" = "UTC"
      }
    }

    # Runs the job's entry script; the deploy workflow uploads it as <volume>/jobs/<job>/job.py.
    # Each YAML argument KEY: value becomes --KEY value on the command line argparse reads.
    spark_python_task {
      python_file = "dbfs:${var.code_volume_path}/jobs/${each.key}/job.py"
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
