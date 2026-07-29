from pyspark.sql.functions import current_timestamp, col, lit , lower,upper, trim, when

def transform_sellers():

    # Read Bronze
    df = spark.read.table("ecommerce.bronze.sellers")
    df_geo = spark.read.table("ecommerce.silver.geolocation")
    print(f"Rows before cleaning: {df.count()}")

    # Clean strings before drop duplicates
    df = (
    df
    .withColumn(
        "seller_city",
        lower(trim(col("seller_city")))
    )
    .withColumn(
        "seller_state",
        upper(trim(col("seller_state")))
    )
    )

    # Drop nulls
    df= df.dropna(subset=["seller_id"])
    df= df.dropDuplicates()

    # Validate geolocation_id
    df = (
        df.join(
            df_geo
            .select("geolocation_zip_code_prefix")
            .dropDuplicates()
            .withColumn("zip_exists", lit(1)),
            df.seller_zip_code_prefix == df_geo.geolocation_zip_code_prefix,
            "left"
        )
        .withColumn(
            "invalid_zip_code_flag",
            when(col("zip_exists").isNull(), 1).otherwise(0)
        )
        .drop("zip_exists", "geolocation_zip_code_prefix")
    )

    df = (df.withColumn("processed_time",current_timestamp())
            .withColumn("source", lit("bronze.sellers")))
    print(f"Rows after cleaning: {df.count()}")

    # Write Silver
    try:
        df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(
        "ecommerce.silver.sellers"
        )
        print("✅ sellers table loaded successfully")
    except Exception as e:
        print(f"❌ Failed to load sellers table: {e}")

transform_sellers()
