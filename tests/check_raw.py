import pandas as pd

file_path = "data/raw/vnstock/ohlcv/FPT/FPT_2024-01-01_2026-09-19.parquet"

df = pd.read_parquet(file_path)

print(df.head())
print()
print(df.columns)
print()
print("Số dòng:", len(df))
print()
print(df.dtypes)