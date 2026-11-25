# SIS 2: Data Streaming and Validation Pipeline (Apache Kafka + Pandera)

**Course:** Data Collection & Preparation (KBTU)
**Data source:** USGS Earthquake Catalog API — earthquakes M4.5+ from 2023-01-01 to 2026-11-18
**Group members:** Tampekov Rassul, Bekmurat Madiyar *(add the rest)*

> This README is a work in progress. SQLite loading, analysis and `main.py` are still to be added.

## Pipeline

```
USGS API -> api_client -> producer -> Kafka topic -> consumer -> validator -> cleaner -> (loader -> SQLite) -> (analysis)
```

| Stage | Script | Input | Output |
|---|---|---|---|
| 1. Collection | `src/api_client.py` | USGS API | `data/raw/pages/*.json`, `data/raw/records.json`, `data/raw_data.csv` |
| 2a. Kafka Producer | `src/producer.py` | `data/raw/records.json` | topic `usgs_earthquakes` |
| 2b. Kafka Consumer | `src/consumer.py` | topic `usgs_earthquakes` | `data/processed/consumed_earthquakes.csv` |
| 3. Validation | `src/validator.py` | consumed CSV | `validated_earthquakes.csv`, `invalid_records.csv` |
| 4. Cleaning | `src/cleaner.py` | validated CSV | `cleaned_earthquakes.csv` + summary CSVs |

## Requirements

- Python 3.10+
- Docker Desktop (for the Kafka broker)

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Kafka setup

`docker-compose.yml` starts one Apache Kafka broker (`apache/kafka:3.8.0`, KRaft mode, no ZooKeeper) on `localhost:9092` and creates the topic `usgs_earthquakes` (1 partition).

```bash
docker compose up -d
docker compose ps        # broker should be "healthy"
```

## API configuration

The USGS API is public: no API key or account needed. Default query parameters (in `src/api_client.py`):

| Parameter | Value |
|---|---|
| Endpoint | `https://earthquake.usgs.gov/fdsnws/event/1/query` |
| `format` | `geojson` |
| `starttime` / `endtime` | `2023-01-01T00:00:00` / `2026-11-18T14:19:58` (fixed, so results are reproducible) |
| `minmagnitude` | `4.5` |
| `eventtype` | `earthquake` |
| `limit` / `offset` | `2000` per page, `offset = 1, 2001, 4001, ...` |

The range is split by year because one USGS query can return at most 20,000 events.

## Run (from the project root folder)

Always run scripts as modules (`python -m src.<name>`), not `python src/<name>.py`.

```bash
python -m src.api_client     # 1. collect data (~1 min)
python -m src.producer       # 2. publish to Kafka
python -m src.consumer       # 3. consume from Kafka (stops after 10 s with no new messages)
python -m src.validator      # 4. Pandera validation
python -m src.cleaner        # 5. cleaning and transformation
```

Optional parameters:

```bash
python -m src.api_client --start 2025-01-01T00:00:00 --end 2025-02-01T00:00:00 --min-mag 5
python -m src.consumer --max-messages 500
```

## Expected output

| Stage | Records |
|---|---|
| Collected from API | 29,631 |
| Published to Kafka | 29,631 |
| Consumed from Kafka | 29,631 |
| Valid / invalid (3 invalid = deliberate test records) | 29,631 / 3 |
| After cleaning | 29,631 |

Note: if the producer is run twice before the consumer, the consumer receives every record twice. The cleaner detects and removes these duplicates by `id`.
