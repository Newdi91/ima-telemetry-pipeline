from pyspark.sql import functions as F
from pyspark.sql.window import Window

def create_silver_dim_plants():
    window_spec = Window.orderBy("plant_location")
    df_dim_plants = (
        spark.read.table("ima.telemetry.bronze_telemetry")    
        .select("plant_location").distinct()
        .withColumn("plant_id", F.row_number().over(window_spec))
    )
    df_dim_plants.write.format("delta").mode("overwrite").saveAsTable("ima.telemetry.silver_dim_plants")

def create_silver_dim_machines():
    df_dim_machines = (
        spark.read.table("ima.telemetry.bronze_telemetry")
        .select("device_id", "machine_model")
        .dropDuplicates(["device_id"])
        .orderBy("device_id")
    )
    df_dim_machines.write.format("delta").mode("overwrite").saveAsTable("ima.telemetry.silver_dim_machines")

def create_silver_fact(df_dim_plants, df_dim_machines):
    silver_fact = (
        spark.readStream.table("ima.telemetry.bronze_telemetry")
        .join(df_dim_plants, on="plant_location", how="inner")
        .join(df_dim_machines, on="device_id", how="inner")
        .select(
            "device_id",
            F.col("pressure_bar").cast("double"),
            F.col("temperature_c").cast("double"),
            F.col("vibration_mm_s").cast("double"),
            F.col("speed_rpm").cast("int"),
            "status_code",
            "plant_id",
            F.col("timestamp").cast("timestamp"),          
        )
    )
    
    query = (
        silver_fact.writeStream.format("delta")
        .option("checkpointLocation", "/Volumes/ima/telemetry/silver/_checkpoint")
        .trigger(availableNow=True)
        .toTable("ima.telemetry.silver_fact")
    )
    
    query.awaitTermination()


if __name__ == "__main__":
    print("Building Silver dimensions...")
    create_silver_dim_plants()
    create_silver_dim_machines()
    
    
    df_plants_loaded = spark.read.table("ima.telemetry.silver_dim_plants")
    df_machines_loaded = spark.read.table("ima.telemetry.silver_dim_machines")
    
    print("Building Silver fact table...")
    create_silver_fact(df_plants_loaded, df_machines_loaded)
    print("Silver layer completed successfully!")