"""Aggregates silver layer facts and dimensions to construct the daily mechanical

health gold table, identifying significant speed drops via window functions and
computing key operational metrics.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def create_gold_mechanical_health():
  """Constructs and saves the daily mechanical health aggregation table for

  machines by joining silver layer facts and dimensions cleanly, calculating
  speed drops using window functions, and computing operational metrics.
  """
  df_fact = spark.read.table("ima.telemetry.silver_fact")
  df_plants = spark.read.table("ima.telemetry.silver_dim_plants")
  # Renaming the device_id column in df_machines to avoid ambiguity during the join
  df_machines = spark.read.table("ima.telemetry.silver_dim_machines").withColumnRenamed(
      "device_id", "dim_device_id"
  )

  
  telemetry_joined = (
      df_fact.join(df_plants, on="plant_id", how="inner")
      .join(df_machines, df_fact["device_id"] == df_machines["dim_device_id"], how="inner")
      .select(
          df_fact["device_id"],
          df_fact["plant_id"],
          df_fact["pressure_bar"],
          df_fact["temperature_c"],
          df_fact["vibration_mm_s"],
          df_fact["speed_rpm"],
          df_fact["status_code"],
          df_fact["timestamp"],
          df_plants["plant_location"],
          df_machines["machine_model"],
      )
  )

  window_spec = Window.partitionBy("device_id").orderBy(
      F.col("timestamp").asc()
  )

  telemetry_with_lag = telemetry_joined.withColumn(
      "prev_speed_rpm", F.lag("speed_rpm", 1).over(window_spec)
  )

  analyzed_drops = telemetry_with_lag.withColumn(
      "is_significant_drop",
      F.when(
          F.col("prev_speed_rpm").isNotNull()
          & ((F.col("prev_speed_rpm") - F.col("speed_rpm")) > 20),
          True,
      ).otherwise(False),
  ).withColumn("operational_date", F.col("timestamp").cast("date"))

  df_mechanical = analyzed_drops.groupBy(
      "device_id", "machine_model", "plant_location", "operational_date"
  ).agg(
      F.round(F.avg("temperature_c"), 2).alias("avg_temperature_c"),
      F.max("temperature_c").alias("max_temperature_c"),
      F.round(F.avg("pressure_bar"), 2).alias("avg_pressure_bar"),
      F.round(F.avg("vibration_mm_s"), 2).alias("avg_vibration_mm_s"),
      F.max("vibration_mm_s").alias("max_vibration_mm_s"),
      F.sum(
          F.when(F.col("is_significant_drop") == True, 1).otherwise(0)
      ).alias("total_speed_drops_count"),
      F.round(
          F.avg(
              F.when(
                  F.col("is_significant_drop"), F.col("temperature_c")
              ).otherwise(None)
          ),
          2,
      ).alias("avg_temp_during_drops_c"),
      F.round(
          F.avg(
              F.when(
                  F.col("is_significant_drop"), F.col("pressure_bar")
              ).otherwise(None)
          ),
          2,
      ).alias("avg_pressure_during_drops_bar"),
  )

  (
      df_mechanical.write.format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable("ima.telemetry.gold_machine_mechanical_health_daily")
  )


if __name__ == "__main__":
  print("Building Gold mechanical health table...")
  create_gold_mechanical_health()
  print("Gold mechanical health layer processed successfully!")