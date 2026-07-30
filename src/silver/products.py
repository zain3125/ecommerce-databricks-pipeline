from pyspark.sql.functions import current_timestamp, col, lit , lower, trim, when

def transform_products(spark):

    # Read Bronze
    df = spark.read.table("ecommerce.bronze.products")
    df_cat = spark.read.table("ecommerce.silver.product_category_name_translation")
    print(f"Rows before cleaning: {df.count()}")

    # Clean strings before drop duplicates
    df = df.withColumn("product_category_name", lower(trim(col("product_category_name"))))

    # Drop nulls
    df= df.dropna(subset=["product_id"])
    df= df.dropDuplicates()

    # Validate category name
    df_cat_check = (
        df_cat
        .select(
            col("product_category_name").alias("category_name")
        )
        .dropDuplicates()
        .withColumn("category_exists", lit(1))
    )

    df = (
        df.join(
            df_cat_check,
            df.product_category_name == df_cat_check.category_name,
            "left"
        )
        .withColumn(
            "invalid_category_name_flag",
            when(col("category_exists").isNull(), 1).otherwise(0)
        )
        .drop("category_exists", "category_name")
    )

    # Handle missing dimensions
    df = df.fillna(
        {
            "product_weight_g": 0,
            "product_length_cm": 0,
            "product_height_cm": 0,
            "product_width_cm": 0
        }
    )

    # Create product volume
    df = df.withColumn("product_volume_cm3",col("product_length_cm") * col("product_height_cm") * col("product_width_cm"))

    df = (df.withColumn("processed_time",current_timestamp())
            .withColumn("source", lit("bronze.products")))
    print(f"Rows after cleaning: {df.count()}")

    # Write Silver
    try:
        df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(
        "ecommerce.silver.products"
        )
        print("✅ products table loaded successfully")
    except Exception as e:
        print(f"❌ Failed to load products table: {e}")
