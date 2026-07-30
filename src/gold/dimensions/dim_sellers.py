from pyspark.sql.functions import col, first

def load_dim_sellers(spark):

    # Read Silver tables
    # Filter out invalid ZIP codes
    df = (
        spark.read.table("ecommerce.silver.sellers")
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
            df.seller_zip_code_prefix == df_geo.geolocation_zip_code_prefix,
            "left"
        )
        .select(
            col("seller_id"),
            col("seller_zip_code_prefix").alias("zip_code_prefix"),
            col("seller_city").alias("city"),
            col("seller_state").alias("state"),
            col("latitude"),
            col("longitude")
        )
    )

    try:
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable("ecommerce.gold.dim_sellers")
        )

        print(f"✅ dim_sellers loaded successfully {df.count()} rows")

    except Exception as e:
        print(f"❌ Error while writing dim_sellers: {e}")
