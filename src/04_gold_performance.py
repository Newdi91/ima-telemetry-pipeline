"""Aggregates silver layer facts and dimensions to construct the daily plant

operational summary gold table, computing overall plant-level averages, max
metrics, and total telemetry records per location and date.
"""

from pyspark.sql import functions as F

def create_gold_operational_performance():
    """Constructs and saves the daily plant operational summary table by joining

  silver fact and dimension tables, then aggregating telemetry metrics by plant
  location and operational date.
  """
    
    fact = spark.read.table("ima.telemetry.silver_fact")
    dim_plants = spark.read.table("ima.telemetry.silver_dim_plants")
    dim_machines = spark.read.table("ima.telemetry.silver_dim_machines")

    df_performance = (
        fact.alias("f")
        .join(dim_plants.alias("p"), on="plant_id", how="inner")
        .join(dim_machines.alias("d"), on="device_id", how="inner")
        .groupBy(
            F.col("d.device_id"),
            F.col("d.machine_model"),
            F.col("p.plant_location"),
            F.col("f.timestamp").cast("date").alias("operational_date")
        )
        .agg(
            F.count(F.lit(1)).alias("total_records_count"),
            F.round(F.avg("f.speed_rpm"), 2).alias("avg_speed_rpm"),
            F.max("f.speed_rpm").alias("max_speed_rpm"),
            F.sum(F.when(F.col("f.status_code") == "OK", 1).otherwise(0)).alias("normal_records_count"),
            F.sum(F.when(F.col("f.status_code") != "OK", 1).otherwise(0)).alias("error_records_count"),
            F.round(
                (F.sum(F.when(F.col("f.status_code") == "OK", 1).otherwise(0)) * 100.0) / F.count(F.lit(1)), 2
            ).alias("operational_efficiency_pct")
        )
    )

    (
        df_performance.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("ima.telemetry.gold_machine_performance_daily")
    )


if __name__ == "__main__":
    print("Building Gold operational performance table...")
    create_gold_operational_performance()
    print("Gold layer processed successfully!")