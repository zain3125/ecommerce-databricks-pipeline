from pyspark.sql import Row
from pyspark.sql.functions import col, year, quarter, month, weekofyear, dayofmonth, date_format, when, current_timestamp, lit, dayofweek
from datetime import date, timedelta


def create_dim_date(spark, start_date="2010-01-01", end_date="2026-12-31"):

    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)

    rows = []

    current = start
    while current <= end:
        rows.append(
            Row(
                date_key=current
            )
        )
        current += timedelta(days=1)

    df = spark.createDataFrame(rows)

    df = (
        df
        .withColumn("year", year(col("date_key")))
        .withColumn("quarter", quarter(col("date_key")))
        .withColumn("month", month(col("date_key")))
        .withColumn("month_name", date_format(col("date_key"), "MMMM"))
        .withColumn("week", weekofyear(col("date_key")))
        .withColumn("day", dayofmonth(col("date_key")))
        .withColumn("day_name", date_format(col("date_key"), "EEEE"))
        .withColumn(
            "is_weekend",
            when(
                dayofweek(col("date_key")).isin(1, 7),
                True
            ).otherwise(False)
        )
        .withColumn("processed_time", current_timestamp())
        .withColumn("source", lit("generated"))
    )

    try:
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable("ecommerce.gold.dim_date")
        )

        print(f"✅ dim_date created successfully ({df.count()} rows)")

    except Exception as e:
        print(f"❌ Error while writing dim_date: {e}")
