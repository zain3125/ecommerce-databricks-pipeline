from pyspark.sql.functions import current_timestamp, col, lit, when, lower, upper, trim

def transform_geolocation():

    # Read Bronze
    df = spark.read.table("ecommerce.bronze.geolocation")
    print(f"Rows before cleaning: {df.count()}")

    # Clean strings before drop duplicates
    df = (
    df
    .withColumn(
        "geolocation_city",
        lower(trim(col("geolocation_city")))
    )
    .withColumn(
        "geolocation_state",
        upper(trim(col("geolocation_state")))
    )
    )

    # Drop nulls & duplicates
    df= df.dropna(subset=["geolocation_zip_code_prefix"])
    df = df.dropDuplicates()

    # Add invalid coordinates flag
    df = df.withColumn(
    "invalid_coordinates_flag",
    when(
        (col("geolocation_lat") < -90)
        |
        (col("geolocation_lat") > 90)
        |
        (col("geolocation_lng") < -180)
        |
        (col("geolocation_lng") > 180),
        1
    )
    .otherwise(0)
    )

    df = (df.withColumn("processed_time",current_timestamp())
            .withColumn("source", lit("bronze.geolocation")))
    print(f"Rows after cleaning: {df.count()}")

    # Write Silver
    try:
        df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(
        "ecommerce.silver.geolocation"
        )
        print("✅ geolocation table loaded successfully")
    except Exception as e:
        print(f"❌ Failed to load geolocation table: {e}")

transform_geolocation()
