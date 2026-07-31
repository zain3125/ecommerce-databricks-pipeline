from pyspark.sql.functions import col

def load_fact_payments(spark):

    # Read Silver payments
    df = (
        spark.read.table("ecommerce.silver.payments")
        .filter(col("invalid_order_id_flag") == 0)
        .filter(col("invalid_payment_installments_flag") == 0)
    )

    # Read Payment Type Dimension
    df_type = (
        spark.read.table("ecommerce.gold.dim_payment_type")
        .select(
            "payment_type_id",
            "payment_type"
        )
    )

    # Replace payment_type with payment_type_id
    df = (
        df.join(
            df_type,
            on="payment_type",
            how="inner"
        )
        .select(
            col("order_id"),
            col("payment_type_id"),
            col("payment_sequential"),
            col("payment_installments"),
            col("payment_value")
        )
    )

    try:
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable("ecommerce.gold.fact_payments")
        )

        print(f"✅ fact_payments loaded successfully ({df.count()} rows)")

    except Exception as e:
        print(f"❌ Error while writing fact_payments: {e}")
