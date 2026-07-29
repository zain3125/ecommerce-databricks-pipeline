from pyspark.sql.functions import current_timestamp, col, lit, when, lower, trim


def transform_order_items():

    # Read Bronze
    df = spark.read.table("ecommerce.bronze.order_items")

    df_orders = spark.read.table("ecommerce.bronze.orders")
    df_products = spark.read.table("ecommerce.silver.products")
    df_sellers = spark.read.table("ecommerce.silver.sellers")

    print(f"Rows before cleaning: {df.count()}")

    # Remove null keys
    df = df.dropna(subset=["order_id", "product_id", "seller_id"])

    df = df.dropDuplicates()


    # Validate order_id
    df = (
        df.join(
            df_orders
            .select("order_id")
            .dropDuplicates()
            .withColumn("order_exists", lit(1)),
            "order_id",
            "left"
        )
        .withColumn(
            "invalid_order_id_flag",
            when(col("order_exists").isNull(), 1).otherwise(0)
        )
        .drop("order_exists")
    )


    # Validate product_id
    df = (
        df.join(
            df_products
            .select("product_id")
            .dropDuplicates()
            .withColumn("product_exists", lit(1)),
            "product_id",
            "left"
        )
        .withColumn(
            "invalid_product_id_flag",
            when(col("product_exists").isNull(), 1).otherwise(0)
        )
        .drop("product_exists")
    )


    # Validate seller_id
    df = (
        df.join(
            df_sellers
            .select("seller_id")
            .dropDuplicates()
            .withColumn("seller_exists", lit(1)),
            "seller_id",
            "left"
        )
        .withColumn(
            "invalid_seller_id_flag",
            when(col("seller_exists").isNull(), 1).otherwise(0)
        )
        .drop("seller_exists")
    )


    df = (df.withColumn("processed_time",current_timestamp())
            .withColumn("source", lit("bronze.order_items")))
    print(f"Rows after cleaning: {df.count()}")

    # Write Silver
    try:
        df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(
        "ecommerce.silver.order_items"
        )
        print("✅ order_items table loaded successfully")
    except Exception as e:
        print(f"❌ Failed to load order_items table: {e}")

transform_order_items()
