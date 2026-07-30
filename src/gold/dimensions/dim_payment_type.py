from pyspark.sql.functions import col, when, row_number
from pyspark.sql.window import Window

def load_payment_type(spark):
    # Read silver table
    df = spark.read.table("ecommerce.silver.payments")

    window = Window.orderBy("payment_type")

    df = (
        df.select("payment_type")
          .dropDuplicates()
          .withColumn(
              "payment_type_id",
              row_number().over(window)
          )
          .select("payment_type_id", "payment_type")
    )

    try:
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable("ecommerce.gold.payment_type")
        )

        print(f"✅ payment_type loaded successfully ({df.count()} rows)")

    except Exception as e:
        print(f"❌ Error while writing payment_type: {e}")
