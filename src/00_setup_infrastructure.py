from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.sql("CREATE CATALOG IF NOT EXISTS ima")
spark.sql("CREATE SCHEMA IF NOT EXISTS ima.telemetry")
spark.sql("CREATE VOLUME IF NOT EXISTS ima.telemetry.landing")
spark.sql("CREATE VOLUME IF NOT EXISTS ima.telemetry.bronze")
spark.sql("CREATE VOLUME IF NOT EXISTS ima.telemetry.silver")

print("Infrastructure created successfully!")