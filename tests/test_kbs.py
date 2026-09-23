from vnstock import Quote


quote = Quote(
    symbol="FPT",
    source="KBS"
)

df = quote.history(
    start="2026-09-01",
    end="2026-09-18",
    interval="1D"
)

print(df)
print()
print(df.columns)
print()
print("Rows:", len(df))