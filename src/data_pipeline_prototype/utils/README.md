* validate_pyspark_dataframe: no change
* create_pyspark_dataframe -> read_raw_files - only used for reading ingested data. change `s3://` path scheme to `abfss://`.
* (new) read_delta_tables -> uses `spark.read.table(table).where(f.col(partition_key).between(start_date, end_date))`.
* _existing_partition_paths: deleted - created as a matter of convenience, not needed for delta tables.
* (new) write_delta_table -> replaces `write_pyspark_dataframe`, uses `writeTo` to avoid having entire table dropped 
  when the partitions are being overwritten. 