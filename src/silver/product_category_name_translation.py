from pyspark.sql.functions import current_timestamp, col, lit , lower, trim

def transform_product_category_name_translation(spark):

    # Read Bronze
    df = spark.read.table("ecommerce.bronze.product_category_name_translation")
    print(f"Rows before cleaning: {df.count()}")

    # Clean strings before drop duplicates
    df = (
    df
    .withColumn(
        "product_category_name",
        lower(trim(col("product_category_name")))
    )
    .withColumn(
        "product_category_name_english",
        lower(trim(col("product_category_name_english")))
    )
    )

    # Drop nulls
    df= df.dropna(subset=["product_category_name"])
    df= df.dropDuplicates(subset=["product_category_name"])

    df = (df.withColumn("processed_time",current_timestamp())
            .withColumn("source", lit("bronze.product_category_name_translation")))
    print(f"Rows after cleaning: {df.count()}")
    # Write Silver
    try:
        df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(
        "ecommerce.silver.product_category_name_translation"
        )
        print("✅ product_category_name_translation table loaded successfully")
    except Exception as e:
        print(f"❌ Failed to load product_category_name_translation table: {e}")
