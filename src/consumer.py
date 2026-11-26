import argparse
import json
import time
from pathlib import Path

import pandas as pd
from confluent_kafka import Consumer, KafkaError

from src.api_client import FIELDS
from src.producer import BOOTSTRAP_SERVERS, TOPIC

GROUP_ID = "sis2-usgs-consumer-group"
IDLE_TIMEOUT = 10  # stop if no new messages for 10s

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_PATH = PROJECT_ROOT / "data" / "consumed" / "consumed_earthquakes.csv"


def consume(max_messages=None):
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])

    records, bad_messages = [], 0
    last_message_time = time.time()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                if time.time() - last_message_time > IDLE_TIMEOUT:
                    print(f"No new messages for {IDLE_TIMEOUT}s -> stopping")
                    break
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(f"Kafka error: {msg.error()}")

            last_message_time = time.time()
            try:
                records.append(json.loads(msg.value().decode("utf-8")))
            except (UnicodeDecodeError, json.JSONDecodeError) as err:
                bad_messages += 1
                print(f"Skipped bad message at offset {msg.offset()}: {err}")

            if max_messages and len(records) >= max_messages:
                print(f"Reached --max-messages={max_messages} -> stopping")
                break

        if records or bad_messages:
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()

    return records, bad_messages


def main():
    parser = argparse.ArgumentParser(description="Kafka consumer for USGS earthquakes")
    parser.add_argument("--max-messages", type=int, default=None)
    args = parser.parse_args()

    records, bad_messages = consume(args.max_messages)
    print(f"Successfully consumed messages: {len(records)}")
    print(f"Bad (not JSON) messages:        {bad_messages}")

    if not records:
        print("No new messages in the topic. Run the producer first.")
        return

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(records, columns=FIELDS).to_csv(OUTPUT_PATH, index=False)
    print(f"Saved -> {OUTPUT_PATH.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()
