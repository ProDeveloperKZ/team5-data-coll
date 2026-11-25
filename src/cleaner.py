"""
Stage 4 - Cleaning and transformation with pandas and NumPy.

Input:  data/processed/validated_earthquakes.csv  (validator output)
Output: data/processed/cleaned_earthquakes.csv    (-> SQLite loader)
        data/processed/summary_by_region.csv
        data/processed/summary_by_year.csv
        data/processed/pivot_year_depth.csv

Run from the project root:
    python -m src.cleaner
"""

from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_PATH = PROJECT_ROOT / "data" / "validated" / "valid_earthquakes.csv"
CLEANED_DIR = PROJECT_ROOT / "data" / "cleaned"
SUMMARY_DIR = CLEANED_DIR / "summaries"
OUTPUT_PATH = CLEANED_DIR / "cleaned_earthquakes.csv"

# How to fill missing values (used only if the column exists in the data)
FILL_RULES = {
    "nst": "median",   # number of stations -> typical value
    "gap": "median",   # azimuthal gap -> typical value
    "dmin": "median",  # distance to nearest station -> typical value
    "felt": 0,         # no "felt" reports means nobody reported feeling it
    "alert": "none",   # no PAGER alert was issued
}
DROP_COLUMN_IF_MISSING_OVER = 0.80  # e.g. mmi, cdi are mostly empty

# Leading direction words in USGS place names, e.g. "south of the Fiji Islands"
DIRECTION_PREFIX = (
    r"^((north|south|east|west|northeast|northwest|southeast|southwest)\s+of(\s+the)?"
    r"|northern|southern|eastern|western|central|near the coast of|off the coast of)\s+"
)
REGION_FIXES = {
    "Fiji Islands": "Fiji",
    "CA": "California",
    "Kahramanmaras earthquake sequence": "Turkey",
}


def handle_missing_values(df):
    print("\n--- 1. Missing values ---")
    missing = df.isna().sum()
    print("Missing values per column (before):")
    print(missing[missing > 0].to_string() if missing.any() else "  none")

    for column, rule in FILL_RULES.items():
        if column in df.columns and df[column].isna().any():
            value = df[column].median() if rule == "median" else rule
            df[column] = df[column].fillna(value)
            print(f"  filled '{column}' with {value}")

    share_missing = df.isna().mean()
    too_empty = share_missing[share_missing > DROP_COLUMN_IF_MISSING_OVER].index.tolist()
    if too_empty:
        df = df.drop(columns=too_empty)
        print(f"  dropped mostly-empty columns: {too_empty}")

    before = len(df)
    df = df.dropna()  # any remaining gap in a required field -> drop the row
    print(f"  rows dropped because of remaining gaps: {before - len(df)}")
    return df


def remove_duplicates(df):
    print("\n--- 2. Duplicates ---")
    duplicates = df.duplicated(subset="id").sum()
    print(f"Duplicate ids found: {duplicates}")
    return df.drop_duplicates(subset="id", keep="first")


def convert_types(df):
    print("\n--- 3. Data types ---")
    df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
    df["updated"] = pd.to_datetime(df["updated"], unit="ms", utc=True)
    df["tsunami"] = df["tsunami"].astype(bool)
    for column in ["magType", "net", "type", "status"]:
        df[column] = df[column].astype("category")
    print(df.dtypes.to_string())
    return df


def normalize_text(df):
    print("\n--- 4. Text normalization ---")
    df["place"] = df["place"].str.strip().str.replace(r"\s+", " ", regex=True)
    df["magType"] = df["magType"].str.strip().str.lower()
    df["net"] = df["net"].str.strip().str.lower()

    # region = text after the last comma ("23 km ESE of Manay, Philippines" -> "Philippines")
    region = df["place"].str.split(",").str[-1].str.strip()
    region = region.str.replace(DIRECTION_PREFIX, "", regex=True, case=False)
    region = region.str.replace(r"\s+region$", "", regex=True)
    region = region.str[0].str.upper() + region.str[1:]
    df["region"] = region.replace(REGION_FIXES)

    print(f"Unique raw place values:  {df['place'].nunique()}")
    print(f"Unique regions after normalization: {df['region'].nunique()}")
    return df


def add_numpy_features(df):
    print("\n--- 5. NumPy operations ---")
    # Seismic energy (Gutenberg-Richter): log10(E) = 1.5 * M + 4.8, E in joules
    df["energy_joules"] = np.power(10.0, 1.5 * df["mag"] + 4.8)
    df["log_energy"] = np.log10(df["energy_joules"])

    # Depth class: shallow < 70 km, intermediate 70-300 km, deep > 300 km
    df["depth_class"] = np.select(
        [df["depth_km"] < 70, df["depth_km"] < 300],
        ["shallow", "intermediate"],
        default="deep",
    )
    df["year"] = df["time"].dt.year

    print(f"Mean magnitude:   {np.mean(df['mag']):.2f}")
    print(f"Std of magnitude: {np.std(df['mag']):.2f}")
    print(f"Total energy released: {np.sum(df['energy_joules']):.3e} J")
    print(df["depth_class"].value_counts().to_string())
    return df


def filter_strong_shallow(df):
    print("\n--- 6. Filtering (multiple conditions) ---")
    strong_shallow = df[(df["mag"] >= 6.0) & (df["depth_class"] == "shallow")]
    print(f"Strong (M>=6) AND shallow (<70 km) earthquakes: {len(strong_shallow)}")
    print(strong_shallow[["time", "mag", "depth_km", "region"]].head().to_string(index=False))
    return strong_shallow


def group_and_reshape(df):
    print("\n--- 7. GroupBy + aggregation ---")
    by_region = (
        df.groupby("region")
        .agg(count=("id", "count"), mean_mag=("mag", "mean"), max_mag=("mag", "max"))
        .sort_values("count", ascending=False)
        .round(2)
    )
    print("Top 10 regions by number of earthquakes:")
    print(by_region.head(10).to_string())

    by_year = df.groupby("year").agg(
        count=("id", "count"), mean_mag=("mag", "mean"), mean_depth=("depth_km", "mean")
    ).round(2)
    print("\nPer year:")
    print(by_year.to_string())

    print("\n--- 8. Reshaping (pivot_table) ---")
    pivot = pd.pivot_table(
        df, index="year", columns="depth_class", values="id", aggfunc="count", fill_value=0
    )
    print(pivot.to_string())

    SUMMARY_DIR.mkdir(parents=True, exist_ok=True)
    by_region.to_csv(SUMMARY_DIR / "summary_by_region.csv")
    by_year.to_csv(SUMMARY_DIR / "summary_by_year.csv")
    pivot.to_csv(SUMMARY_DIR / "pivot_year_depth.csv")


def main():
    df = pd.read_csv(INPUT_PATH)
    print(f"Records received from validator: {len(df)}")

    df = handle_missing_values(df)
    df = remove_duplicates(df)
    df = convert_types(df)
    df = normalize_text(df)
    df = add_numpy_features(df)
    filter_strong_shallow(df)
    group_and_reshape(df)

    df.to_csv(OUTPUT_PATH, index=False)
    print(f"\nRecords after cleaning: {len(df)}")
    print(f"Saved -> {OUTPUT_PATH.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()
