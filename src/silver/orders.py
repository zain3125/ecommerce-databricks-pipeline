from pyspark.sql.functions import current_timestamp, to_timestamp, to_date, col, lit, when, datediff

def transform_orders(spark):

    # Read Bronze
    df = spark.read.table("ecommerce.bronze.orders")
    print(f"Rows before cleaning: {df.count()}")

    # Transform
    # Convert timestamp data types
    df = df.withColumn("order_purchase_timestamp",to_timestamp(col("order_purchase_timestamp")))
    df = df.withColumn("order_approved_at",to_timestamp(col("order_approved_at")))
    df = df.withColumn("order_delivered_carrier_date",to_timestamp(col("order_delivered_carrier_date")))
    df = df.withColumn("order_delivered_customer_date",to_timestamp(col("order_delivered_customer_date")))
    df = df.withColumn("order_estimated_delivery_date",to_date(col("order_estimated_delivery_date")))

    # Drop nulls
    df= df.dropna(subset=["order_id"])
    df = df.dropna(subset=["customer_id"])
    df= df.dropDuplicates(["order_id"])
    # Approved at flag
    df = df.withColumn(
    "missing_approved_at_flag",
    when(
        (col("order_status").isin(
            "approved",
            "invoiced",
            "processing",
            "shipped",
            "delivered"
        ))
        &
        (col("order_approved_at").isNull()),
        1
    )
    .otherwise(0)
    )
    # Carrier date flag
    df = df.withColumn(
    "missing_carrier_date_flag",
    when(
        (col("order_status").isin(
            "shipped",
            "delivered"
        ))
        &
        (col("order_delivered_carrier_date").isNull()),
        1
    )
    .otherwise(0)
    )
    # Customer delivery date flag
    df = df.withColumn(
    "missing_customer_delivery_flag",
    when(
        (col("order_status") == "delivered")
        &
        (col("order_delivered_customer_date").isNull()),
        1
    )
    .otherwise(0)
    )
    
    # Business rules
    df = df.withColumn(
    "delivery_duration_days",
    datediff(
        col("order_delivered_customer_date"),
        col("order_purchase_timestamp")
    )
    )

    df = df.withColumn(
    "approval_delay_hours",
    (
        col("order_approved_at").cast("long")
        -
        col("order_purchase_timestamp").cast("long")
    ) / 3600
    )
    
    df = df.withColumn(
    "delivery_delay_days",
    datediff(
        col("order_delivered_customer_date"),
        col("order_estimated_delivery_date")
    )
    )

    df = (df.withColumn("processed_time",current_timestamp())
            .withColumn("source", lit("bronze.orders")))

    print(f"Rows after cleaning: {df.count()}")
    # Write Silver
    try:
        df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(
        "ecommerce.silver.orders"
        )
        print("✅ orders table loaded successfully")
    except Exception as e:
        print(f"❌ Failed to load orders table: {e}")
