from src.config import RAW_PATH, BRONZE_PATH, TABLES

raw_path = RAW_PATH
bronze_path = BRONZE_PATH

tables = TABLES

for name, file in tables.items():
    input_path = f"{raw_path}/{file}"
    output_path = f"{bronze_path}/{name}"
    print(f'✅ {name} start')
    df = (
        spark.read
        .option('header',True)
        .option('inferschema',True)
        .csv(input_path)
    )
    df.write.format("delta").mode("overwrite").saveAsTable(f"ecommerce.bronze.{name}")
    
    print(f'✅ {name} end')
