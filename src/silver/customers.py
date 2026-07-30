from pyspark.sql.functions import ( current_timestamp, trim, upper, col, lit, when)

def transform_customers(spark):

    # Read Bronze
    df = spark.read.table("ecommerce.bronze.customers")
    df_geo = spark.read.table("ecommerce.silver.geolocation")

    print(f"Rows before cleaning: {df.count()}")

    # Clean data
    df = (
        df
        .dropna(subset=["customer_id"])
        .dropDuplicates(["customer_id"])
        .withColumn("customer_city", trim(col("customer_city")))
        .withColumn("customer_state", upper(trim(col("customer_state"))))
    )

    # Validate ZIP code
    df = (
        df.join(
            df_geo
            .select("geolocation_zip_code_prefix")
            .dropDuplicates()
            .withColumn("zip_exists", lit(1)),
            df.customer_zip_code_prefix == df_geo.geolocation_zip_code_prefix,
            "left"
        )
        .withColumn(
            "invalid_zip_code_flag",
            when(col("zip_exists").isNull(), 1).otherwise(0)
        )
        .drop("zip_exists", "geolocation_zip_code_prefix")
    )

    # Metadata
    df = (
        df.withColumn("processed_time", current_timestamp())
          .withColumn("source", lit("bronze.customers"))
    )

    print(f"Rows after cleaning: {df.count()}")

    # Write Silver
    try:
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable("ecommerce.silver.customers")
        )

        print("✅ Customers table loaded successfully")

    except Exception as e:
        print(f"❌ Failed to load customers table: {e}")
