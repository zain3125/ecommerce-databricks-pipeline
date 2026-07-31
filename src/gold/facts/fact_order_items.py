from pyspark.sql.functions import col

def load_fact_order_items(spark):

    # Read Silver order_items
    df = (
        spark.read.table("ecommerce.silver.order_items")
        .filter(col("invalid_order_id_flag") == 0)
        .filter(col("invalid_product_id_flag") == 0)
        .filter(col("invalid_seller_id_flag") == 0)
    )

    df = df.select(
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "price",
            "freight_value"
        )

    try:
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable("ecommerce.gold.fact_order_items")
        )

        print(f"✅ fact_order_items loaded successfully ({df.count()} rows)")

    except Exception as e:
        print(f"❌ Error while writing fact_order_items: {e}")
