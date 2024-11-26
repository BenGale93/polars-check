# /// script
# requires-python = ">=3.12"
# dependencies = ["polars"]
# ///
import polars as pl

with pl.Config(tbl_cols=-1):
    print(
        pl.scan_parquet("manual_testing/yellow_tripdata_2023-01.parquet")
        .head(5)
        .collect()
    )
