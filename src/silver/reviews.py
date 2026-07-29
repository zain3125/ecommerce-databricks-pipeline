from pyspark.sql.functions import current_timestamp, col, lit, when , to_timestamp, to_date, lower, trim

def transform_reviews():

    # Read Bronze
    df = spark.read.table("ecommerce.bronze.reviews")
    orders_df = spark.read.table("ecommerce.bronze.orders")
    print(f"Rows before cleaning: {df.count()}")
    
    # Convert timestamp data types
    df = df.withColumn("review_answer_timestamp",to_timestamp(col("review_answer_timestamp")))
    df = df.withColumn("review_creation_date",to_date(col("review_creation_date")))

    # Clean strings before drop duplicates
    df = (
    df
    .withColumn(
        "review_comment_title",
        lower(trim(col("review_comment_title")))
    )
    .withColumn(
        "review_comment_message",
        lower(trim(col("review_comment_message")))
    )
    )

    # Drop nulls
    df= df.dropna(subset=["review_id"])
    df= df.dropna(subset=["order_id"])
    df= df.dropDuplicates()

    # Validate order_id
    df = (df
    .join(
        orders_df.select("order_id")
        .withColumn("order_exists", lit(1)),
        on="order_id",
        how="left"
    )
    .withColumn(
        "invalid_order_id_flag",
        when(
            col("order_exists").isNull(),
            1
        )
        .otherwise(0)
    )
    .drop("order_exists")
    )

    df = (df.withColumn("processed_time",current_timestamp())
            .withColumn("source", lit("bronze.reviews")))
    print(f"Rows after cleaning: {df.count()}")
    # Write Silver
    try:
        df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(
        "ecommerce.silver.reviews"
        )
        print("✅ reviews table loaded successfully")
    except Exception as e:
        print(f"❌ Failed to load reviews table: {e}")

transform_reviews()
