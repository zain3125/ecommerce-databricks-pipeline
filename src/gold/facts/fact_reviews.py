from pyspark.sql.functions import col

def load_fact_reviews(spark):

    # Read Silver reviews
    df = (
        spark.read.table("ecommerce.silver.reviews")
        .filter(col("invalid_order_id_flag") == 0)
    )

    df = df.select(
            "review_id",
            "order_id",
            "review_score",
            "review_creation_date",
            "review_answer_timestamp"
        )

    try:
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable("ecommerce.gold.fact_reviews")
        )

        print(f"✅ fact_reviews loaded successfully ({df.count()} rows)")

    except Exception as e:
        print(f"❌ Error while writing fact_reviews: {e}")
