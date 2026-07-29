from pyspark.sql.functions import current_timestamp, col, lit, when

def transform_payments():

    # Read Bronze
    df = spark.read.table("ecommerce.bronze.payments")
    orders_df = spark.read.table("ecommerce.bronze.orders")
    print(f"Rows before cleaning: {df.count()}")

    # Drop nulls
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

    # Validate payment installments
    df = df.withColumn(
    "invalid_payment_installments_flag",
    when(
        col("payment_installments").isNull()
        |
        (col("payment_installments") <= 0),
        1
    )
    .otherwise(0)
    )

    df = (df.withColumn("processed_time",current_timestamp())
            .withColumn("source", lit("bronze.payments")))
    print(f"Rows after cleaning: {df.count()}")

    # Write Silver
    try:
        df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(
        "ecommerce.silver.payments"
        )
        print("✅ payments table loaded successfully")
    except Exception as e:
        print(f"❌ Failed to load payments table: {e}")

transform_payments()
