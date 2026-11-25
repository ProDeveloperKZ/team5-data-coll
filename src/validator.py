"""
Stage 3 - Pandera validation of the records received from Kafka.

Input:  data/processed/consumed_earthquakes.csv   (Consumer output)
Output: data/processed/validated_earthquakes.csv  (valid real records -> cleaner)
        data/processed/invalid_records.csv        (invalid records + their errors)

Run from the project root:
    python -m src.validator
"""

import time
from pathlib import Path

import pandas as pd
import pandera.pandas as pa

PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_PATH = PROJECT_ROOT / "data" / "consumed" / "consumed_earthquakes.csv"
VALID_PATH = PROJECT_ROOT / "data" / "validated" / "valid_earthquakes.csv"
INVALID_PATH = PROJECT_ROOT / "data" / "validated" / "invalid_records.csv"

ALLOWED_MAG_TYPES = ["mb", "md", "ml", "ml(texnet)", "mw", "mwb", "mwc", "mwp", "mwr", "mww"]
ALLOWED_STATUSES = ["reviewed", "automatic"]
ALLOWED_ALERTS = ["green", "yellow", "orange", "red"]
NOW_MS = int(time.time() * 1000)

# Validation schema: one rule (or more) per column
schema = pa.DataFrameSchema(
    columns={
        # Rule 1: identifier is required and has the USGS format (lowercase letters/digits).
        # Duplicate ids are removed later in the cleaner.
        "id": pa.Column(str, pa.Check.str_matches(r"^[a-z0-9]+$"), nullable=False),
        # Rule 2: numerical range (only M4.5+ were requested from the API)
        "mag": pa.Column(float, pa.Check.in_range(4.5, 10.0)),
        # Rule 3: allowed categorical values
        "magType": pa.Column(str, pa.Check.isin(ALLOWED_MAG_TYPES)),
        "net": pa.Column(str, nullable=False),
        "place": pa.Column(str, nullable=False),
        # Rule 4: date/time - positive Unix ms and not in the future
        "time": pa.Column(int, [pa.Check.gt(0), pa.Check.le(NOW_MS)]),
        "updated": pa.Column(int, pa.Check.gt(0)),
        "type": pa.Column(str, pa.Check.eq("earthquake")),
        "status": pa.Column(str, pa.Check.isin(ALLOWED_STATUSES)),
        "tsunami": pa.Column(int, pa.Check.isin([0, 1])),
        "sig": pa.Column(int, pa.Check.in_range(0, 3000)),
        # Optional fields: missing values are allowed here, the cleaner handles them
        "felt": pa.Column(float, pa.Check.ge(0), nullable=True, coerce=True),
        "cdi": pa.Column(float, pa.Check.in_range(0, 12), nullable=True, coerce=True),
        "mmi": pa.Column(float, pa.Check.in_range(0, 12), nullable=True, coerce=True),
        "alert": pa.Column(str, pa.Check.isin(ALLOWED_ALERTS), nullable=True),
        "nst": pa.Column(float, pa.Check.ge(0), nullable=True, coerce=True),
        "gap": pa.Column(float, pa.Check.in_range(0, 360), nullable=True, coerce=True),
        "dmin": pa.Column(float, pa.Check.ge(0), nullable=True, coerce=True),
        # Rule 5: geographic coordinate boundaries
        "latitude": pa.Column(float, pa.Check.in_range(-90, 90)),
        "longitude": pa.Column(float, pa.Check.in_range(-180, 180)),
        "depth_km": pa.Column(float, pa.Check.in_range(-10, 800)),
        "is_test_record": pa.Column(bool),
    },
    # Rule 6: custom cross-column condition
    checks=pa.Check(lambda df: df["updated"] >= df["time"], error="updated must be >= time"),
)


def add_test_records(df):
    """Add 3 deliberately invalid records, each breaking a different rule."""
    base = df.iloc[0].to_dict()

    future_ms = NOW_MS + 365 * 24 * 3600 * 1000

    bad_mag = {**base, "id": "testbadmag", "mag": 15.0}                    # breaks mag range
    bad_lat = {**base, "id": "testbadlat", "latitude": 200.0}              # breaks latitude range
    bad_time = {**base, "id": "testbadtime", "time": future_ms,
                "updated": future_ms}                                       # breaks "not in future"

    test_rows = pd.DataFrame([bad_mag, bad_lat, bad_time])
    test_rows["is_test_record"] = True
    return pd.concat([df, test_rows], ignore_index=True)


def validate(df):
    """Validate all rows. Return (valid_df, invalid_df, failure_cases)."""
    try:
        schema.validate(df, lazy=True)  # lazy=True -> collect ALL errors, not just the first
        return df, df.iloc[0:0], pd.DataFrame()
    except pa.errors.SchemaErrors as err:
        failures = err.failure_cases
        bad_index = failures["index"].dropna().astype(int).unique()

        invalid_df = df.loc[bad_index].copy()
        # attach the error message(s) to each invalid row
        errors_per_row = failures.dropna(subset=["index"]).groupby("index")["check"].apply(
            lambda checks: "; ".join(map(str, checks))
        )
        invalid_df["validation_errors"] = errors_per_row.reindex(invalid_df.index).values

        valid_df = df.drop(index=bad_index)
        return valid_df, invalid_df, failures


def main():
    df = pd.read_csv(INPUT_PATH)
    df["is_test_record"] = False
    print(f"Records received from Kafka consumer: {len(df)}")

    df = add_test_records(df)
    print(f"Records after adding 3 test records:  {len(df)}")

    valid_df, invalid_df, failures = validate(df)

    # Test records are always excluded from the final dataset
    valid_df = valid_df[~valid_df["is_test_record"]].drop(columns="is_test_record")

    print("\n" + "=" * 60)
    print("PANDERA VALIDATION RESULTS")
    print("=" * 60)
    print(f"Valid records:   {len(valid_df)}")
    print(f"Invalid records: {len(invalid_df)}")
    print(f"  of which test records: {int(invalid_df['is_test_record'].sum())}")
    print(f"  of which real records: {int((~invalid_df['is_test_record']).sum())}")

    if not failures.empty:
        print("\nValidation errors found:")
        print(failures[["column", "check", "failure_case", "index"]].to_string(index=False))

    VALID_PATH.parent.mkdir(parents=True, exist_ok=True)
    valid_df.to_csv(VALID_PATH, index=False)
    invalid_df.to_csv(INVALID_PATH, index=False)
    print(f"\nSaved valid records   -> {VALID_PATH.relative_to(PROJECT_ROOT)}")
    print(f"Saved invalid records -> {INVALID_PATH.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()
