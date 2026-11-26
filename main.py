"""
Run the complete SIS 2 pipeline from start to finish:
1. API collection (src.api_client)
2. Kafka Producer (src.producer)
3. Kafka Consumer (src.consumer)
4. Pandera Validation (src.validator)
5. Data Cleaning & Transformation (src.cleaner)
6. SQLite3 Database Loading (src.loader)
7. Exploratory Data Analysis & Plots (src.analyze)

Usage:
    python main.py
    python main.py --skip-collect
"""

import argparse
import subprocess
import sys


STAGES = [
    ("Stage 1: USGS API Collection", [sys.executable, "-m", "src.api_client"]),
    ("Stage 2a: Kafka Producer", [sys.executable, "-m", "src.producer"]),
    ("Stage 2b: Kafka Consumer", [sys.executable, "-m", "src.consumer"]),
    ("Stage 3: Pandera Validation", [sys.executable, "-m", "src.validator"]),
    ("Stage 4: Cleaning & Transformation", [sys.executable, "-m", "src.cleaner"]),
    ("Stage 5: SQLite3 Database Loader", [sys.executable, "-m", "src.loader"]),
    ("Stage 6: Exploratory Data Analysis", [sys.executable, "-m", "src.analyze"]),
]


def main():
    parser = argparse.ArgumentParser(description="Run the complete SIS 2 pipeline.")
    parser.add_argument(
        "--skip-collect",
        action="store_true",
        help="Skip Stage 1 (API collection) and use existing data/raw/records.json",
    )
    args = parser.parse_args()

    stages = STAGES[1:] if args.skip_collect else STAGES
    for title, cmd in stages:
        print(f"\n========== {title} ==========")
        result = subprocess.run(cmd)
        if result.returncode != 0:
            print(f"Stage failed ({title}) with exit code {result.returncode}")
            sys.exit(result.returncode)

    print("\nPipeline finished successfully.")


if __name__ == "__main__":
    main()
