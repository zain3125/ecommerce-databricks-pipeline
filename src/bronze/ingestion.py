from src.config import RAW_PATH, TABLES

raw_path = RAW_PATH
tables = TABLES

def ingest(spark, raw_path=RAW_PATH, tables=TABLES):
    for name, file in tables.items():
        input_path = f"{raw_path}/{file}"

        print(f"✅ {name} start")

        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .option("multiLine", True)
            .option("quote", '"')
            .option("escape", '"')
            .option("mode", "PERMISSIVE")
            .csv(input_path)
        )

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(f"ecommerce.bronze.{name}")
        )

        print(f"✅ {name} end")
