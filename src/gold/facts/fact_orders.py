from pyspark.sql.functions import col, to_date

def load_fact_orders(spark):

    # Read Silver orders
    df = (
        spark.read.table("ecommerce.silver.orders")
        .filter(col("missing_approved_at_flag") == 0)
        .filter(col("missing_carrier_date_flag") == 0)
        .filter(col("missing_customer_delivery_flag") == 0)
    )

    # Create date keys
    df = (
        df
        .withColumn(
            "purchase_date_key",
            to_date(col("order_purchase_timestamp"))
        )
        .withColumn(
            "approved_date_key",
            to_date(col("order_approved_at"))
        )
        .withColumn(
            "estimated_delivery_date_key",
            col("order_estimated_delivery_date")
        )
        .withColumn(
            "delivered_date_key",
            to_date(col("order_delivered_customer_date"))
        )
    )

    df = df.select(
        "order_id",
        "customer_id",
        "order_status",
        "purchase_date_key",
        "approved_date_key",
        "estimated_delivery_date_key",
        "delivered_date_key",
        "delivery_duration_days",
        "approval_delay_hours",
        "delivery_delay_days"
    )

    try:
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable("ecommerce.gold.fact_orders")
        )

        print(f"✅ fact_orders loaded successfully ({df.count()} rows)")

    except Exception as e:
        print(f"❌ Error while writing fact_orders: {e}")
