import os

from pyspark.sql import SparkSession

__doc__ = """Custom Spark for local dev """
__all__ = ["setup_local_session","spark"]

def setup_local_session():
    """Setup local session as close as ADB 10.4LTS"""
    root = os.path.dirname(os.path.realpath(os.getcwd()))
    spark = (
        SparkSession.builder.appName("Local")
        .master("local[*]")
        .enableHiveSupport()
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.execution.arrow.pyspark.enabled", True)
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", True)
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.debug.maxToStringFields", 10000)
        .config("spark.memory.offHeap.enabled", True)
        .config("spark.memory.offHeap.size", "12g")
    )
    return spark

# Get Spark Session main function
if os.environ.get("LOCAL_DEV", False):
    spark = setup_local_session()
else:
    spark = SparkSession.builder
spark = spark.getOrCreate()