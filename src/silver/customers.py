from pyspark.sql.functions import current_timestamp, trim, upper, col, lit

def transform_customers():

    # Read Bronze
    df = spark.read.table("ecommerce.bronze.customers")
    print(f"Rows before cleaning: {df.count()}")
    # Transform
    df = df.dropna(subset=["customer_id"])
    df = df.dropDuplicates(["customer_id"])
    df = (
        df
        .withColumn("customer_city",trim(col("customer_city")))
        .withColumn("customer_state",upper(trim(col("customer_state"))))
    )
    df = (df.withColumn("processed_time",current_timestamp())
            .withColumn("source", lit("bronze.customers")))
    
    print(f"Rows after cleaning: {df.count()}")
    # Write Silver
    try:
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable("ecommerce.silver.customers")
        print("✅ Customers table loaded successfully")

    except Exception as e:
        print(f"❌ Failed to load customers table: {e}")
transform_customers()
