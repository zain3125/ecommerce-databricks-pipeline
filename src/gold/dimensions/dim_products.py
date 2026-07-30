from pyspark.sql.functions import col

def load_dim_products(spark):

    # Read Silver tables
    # Filter out products with invalid category name
    df = (
        spark.read.table("ecommerce.silver.products")
        .filter(col("invalid_category_name_flag") == 0)
    )

    df_cat = (
        spark.read.table(
            "ecommerce.silver.product_category_name_translation"
        )
        .select(
            "product_category_name",
            "product_category_name_english"
        )
        .dropDuplicates()
    )

    # Join products with category translation
    df = (
        df.join(
            df_cat,
            "product_category_name",
            "left"
        )
        .select(
            col("product_id"),
            col("product_category_name").alias("category_name"),
            col("product_category_name_english").alias("category_name_english"),
            col("product_name_lenght").alias("name_length"),
            col("product_description_lenght").alias("description_length"),
            col("product_photos_qty").alias("photos_qty"),
            col("product_weight_g").alias("weight_g"),
            col("product_length_cm").alias("length_cm"),
            col("product_height_cm").alias("height_cm"),
            col("product_width_cm").alias("width_cm"),
            col("product_volume_cm3").alias("volume_cm3")
        )
    )

    try:
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable("ecommerce.gold.dim_products")
        )

        print(f"✅ dim_products loaded successfully ({df.count()} rows)")

    except Exception as e:
        print(f"❌ Error while writing dim_products: {e}")
