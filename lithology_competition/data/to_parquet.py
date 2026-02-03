#%% Imports
import polars as pl
from pathlib import Path
#%% 
data_path: Path = Path(__file__).parent
data_train: pl.LazyFrame = pl.scan_csv(data_path / "train.csv", separator=";")
data_test_feat: pl.LazyFrame = pl.scan_csv(data_path / "leaderboard_test_features.csv", separator=";")
data_test_target: pl.LazyFrame = pl.scan_csv(data_path / "leaderboard_test_target.csv", separator=";")
# %%
data_train.head()
# %%
data_train.schema
# %%
data_test_combined: pl.LazyFrame = data_test_feat.join(data_test_target, on=["WELL", "DEPTH_MD"])
# %%
data_test_combined.collect_schema()
# %%
data_train.collect_schema().names()
# %%
stats = data_train.select(pl.col("FORCE_2020_LITHOFACIES_CONFIDENCE")).collect().describe()
print(stats)
# %%
data_train_cleaned: pl.LazyFrame = data_train.drop("FORCE_2020_LITHOFACIES_CONFIDENCE")
#%%
cols_to_fix = [
    "NPHI", 
    "MUDWEIGHT", 
    "SGR", 
    "RSHA", 
    "RMIC",
    "ROPA", 
    "RXO", 
    "BS", 
    "DTS", 
    "DCAL"
]
#%%
data_train_cleaned = data_train_cleaned.with_columns(
    pl.lit("train").alias("dataset")
)
data_test_combined = data_test_combined.with_columns(
    pl.lit("test").alias("dataset")
)
#%%
data_train_fixed: pl.LazyFrame = data_train_cleaned.with_columns(
    pl.col(cols_to_fix).cast(pl.Float64, strict=False)
)
data_test_fixed: pl.LazyFrame = data_test_combined.with_columns(
    pl.col(cols_to_fix).cast(pl.Float64, strict=False)
)
# %%
data_all: pl.LazyFrame = pl.concat([data_train_fixed, data_test_fixed])
data_all.sink_parquet(data_path / "lithology_data.parquet")

# %%
# Get unique well names and export each one lazily
well_names = data_all.select("WELL").unique().collect().get_column("WELL").to_list()

wells_dir = data_path / "wells"
wells_dir.mkdir(exist_ok=True)

for well in well_names:
    safe_name = str(well).replace("/", "-").replace(" ", "_")
    (
        data_all
        .filter(pl.col("WELL") == well)
        .sink_parquet(wells_dir / f"{safe_name}.parquet")
    )

# %%
data_train_fixed.collect_schema()
# %%
data_test_fixed.collect_schema()
# %%
