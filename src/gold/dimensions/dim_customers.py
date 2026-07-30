from pyspark.sql.functions import col, first

def load_dim_customers(spark):

    # Read Silver tables
    # Filter out invalid ZIP codes
    df = (
        spark.read.table("ecommerce.silver.customers")
        .filter(col("invalid_zip_code_flag") == 0)
    )
    # Keep one geolocation record per ZIP code
    df_geo = (
        spark.read.table("ecommerce.silver.geolocation")
        .groupBy("geolocation_zip_code_prefix")
        .agg(
            first("geolocation_lat").alias("latitude"),
            first("geolocation_lng").alias("longitude")
        )
    )

    df = (
        df.join(
            df_geo,
            df.customer_zip_code_prefix == df_geo.geolocation_zip_code_prefix,
            "left"
        )
        .select(
            col("customer_id"),
            col("customer_unique_id"),
            col("customer_zip_code_prefix").alias("zip_code_prefix"),
            col("customer_city").alias("city"),
            col("customer_state").alias("state"),
            col("latitude"),
            col("longitude")
        )
    )

    try:
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable("ecommerce.gold.dim_customers")
        )

        print(f"✅ dim_customers loaded successfully {df.count()} rows")

    except Exception as e:
        print(f"❌ Error while writing dim_customers: {e}")
