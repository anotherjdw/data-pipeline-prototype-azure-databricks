from pyspark.sql import SparkSession


def spark_session() -> SparkSession:
    return SparkSession.builder.getOrCreate()
