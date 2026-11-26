import argparse
import subprocess
import sys

STAGES = [
    ("1. API Collection", [sys.executable, "-m", "src.api_client"]),
    ("2a. Kafka Producer", [sys.executable, "-m", "src.producer"]),
    ("2b. Kafka Consumer", [sys.executable, "-m", "src.consumer"]),
    ("3. Pandera Validation", [sys.executable, "-m", "src.validator"]),
    ("4. Cleaning & Transformation", [sys.executable, "-m", "src.cleaner"]),
    ("5. SQLite3 Loader", [sys.executable, "-m", "src.loader"]),
    ("6. EDA & Plots", [sys.executable, "-m", "src.analyze"]),
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-collect", action="store_true")
    args = parser.parse_args()

    stages = STAGES[1:] if args.skip_collect else STAGES
    for title, cmd in stages:
        print(f"\n--- {title} ---")
        res = subprocess.run(cmd)
        if res.returncode != 0:
            sys.exit(res.returncode)

    print("\nDone.")


if __name__ == "__main__":
    main()
