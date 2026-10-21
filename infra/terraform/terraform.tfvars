project     = "data-pipeline-prototype"
environment = "prod"
location    = "germanywestcentral"
container_names = {
  raw          = "raw"
  preprocessed = "preprocessed"
  processed    = "processed"
  artifacts    = "artifacts"
}
wheel_file_name  = "data_pipeline_prototype-0.1.0-py3-none-any.whl"
schedules_paused = true # format_orders runs a fixed backfill for now; unpause once it uses lookback
