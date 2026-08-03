def ingest_landing_to_bronze():
    """
    Reads JSON telemetry files incrementally from the Unity Catalog landing volume 
    using Auto Loader (cloudFiles) and writes them to the Bronze Delta table.
    """
    bronze_telemetry = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "*.json")
        .option("cloudFiles.schemaLocation", "/Volumes/ima/telemetry/landing/_schema")
        .load("/Volumes/ima/telemetry/landing")
    )

    query = (
        bronze_telemetry.writeStream.format("delta")
        .option("checkpointLocation", "/Volumes/ima/telemetry/landing/_checkpoint")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable("ima.telemetry.bronze_telemetry")
    )
    
    
    query.awaitTermination()


if __name__ == "__main__":
    print("Building Bronze layer...")
    ingest_landing_to_bronze()    
    print("Bronze layer completed successfully!")
    