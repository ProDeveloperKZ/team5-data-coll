# SIS 2: Data Streaming and Validation Pipeline (Apache Kafka + Pandera)

**Course:** Data Collection & Preparation (KBTU)
**Data source:** USGS Earthquake Catalog API — earthquakes M4.5+ from 2023-01-01 to 2026-11-18
**Group members (Team 5):** Tampekov Rassul (23B031449), Bekmurat Madiyar (23B030035), Kabanbay Zhamshid (23B031297)

## Pipeline

```
USGS API -> api_client -> producer -> Kafka topic -> consumer -> validator -> cleaner -> loader (SQLite3) -> analyze (EDA)
```

| Stage | Script | Input | Output |
|---|---|---|---|
| 1. Collection | `src/api_client.py` | USGS API | `data/raw/pages/*.json`, `data/raw/records.json`, `data/raw_data.csv` |
| 2a. Kafka Producer | `src/producer.py` | `data/raw/records.json` | topic `usgs_earthquakes` |
| 2b. Kafka Consumer | `src/consumer.py` | topic `usgs_earthquakes` | `data/consumed/consumed_earthquakes.csv` |
| 3. Validation | `src/validator.py` | `data/consumed/consumed_earthquakes.csv` | `data/validated/valid_earthquakes.csv`, `data/validated/invalid_records.csv` |
| 4. Cleaning | `src/cleaner.py` | `data/validated/valid_earthquakes.csv` | `data/cleaned/cleaned_earthquakes.csv`, `data/cleaned/summaries/*.csv` |
| 5. SQLite3 Loader | `src/loader.py` | `data/cleaned/cleaned_earthquakes.csv` | `output.db` (`Records` table, `id TEXT PRIMARY KEY`) |
| 6. EDA & Plots | `src/analyze.py` | `output.db` | 8 visualizations in `figures/*.png` |

## Requirements

- Python 3.10+
- Docker Desktop (for the Kafka broker)

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Kafka setup

`docker-compose.yml` starts one Apache Kafka broker (`apache/kafka:3.8.0`, KRaft mode, no ZooKeeper) on `localhost:9092` with persistent volume `sis2_usgs_kafka_data` and creates the topic `usgs_earthquakes` (1 partition).

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
| `orderby` | `time-asc` |
| `limit` / `offset` | `2000` per page, `offset = 1, 2001, 4001, ...` |

The range is split by year (2023, 2024, 2025, 2026) because one USGS query can return at most 20,000 events.

## Run (from the project root folder)

Always run scripts from the repository root folder:

```bash
python -m src.api_client     # 1. collect data from USGS API (17 pages -> 29,675 records)
python -m src.producer       # 2. publish 29,675 JSON messages to Kafka
python -m src.consumer       # 3. consume 29,675 messages from Kafka (stops after 10 s idle)
python -m src.validator      # 4. Pandera validation (29,675 valid + 3 invalid test rows)
python -m src.cleaner        # 5. cleaning, region normalization, NumPy features, GroupBy & pivot_table
python -m src.loader         # 6. load into SQLite3 (output.db), run double-insert idempotency test & SQL checks
python -m src.analyze        # 7. run exploratory data analysis and save 8 plots to figures/
```

Or run the entire pipeline with `main.py`:

```bash
python main.py               # full pipeline (Stages 1-6)
python main.py --skip-collect # run Stages 2-6 using existing data/raw/records.json
```

Optional parameters:

```bash
python -m src.api_client --start 2025-01-01T00:00:00 --end 2025-02-01T00:00:00 --min-mag 5
python -m src.consumer --max-messages 500
```

## Expected output and verification

| Stage | Records | Verification |
|---|---|---|
| 1. Collected from API | 29,675 | `data/raw/records.json` and `data/raw_data.csv` (2023: 7,643; 2024: 6,397; 2025: 8,559; 2026: 7,076) |
| 2a. Published to Kafka | 29,675 | `Successfully published messages: 29675`, `Failed messages: 0` |
| 2b. Consumed from Kafka | 29,675 | `data/consumed/consumed_earthquakes.csv` (`29,675` rows) |
| 3. Valid / invalid | 29,675 / 3 | `data/validated/valid_earthquakes.csv` (`29,675` rows), `data/validated/invalid_records.csv` (`3` test rows) |
| 4. After cleaning | 29,675 | `data/cleaned/cleaned_earthquakes.csv` (`29,675` rows, 24 columns) + 3 summary CSVs |
| 5. Stored in SQLite3 (1st & 2nd insert) | 29,675 | `output.db` (`SELECT COUNT(id) FROM Records` = `29675` after both 1st and 2nd insert) |
| 6. EDA & Visualizations | 29,675 | 8 PNG charts generated in `figures/` |

Full documentation of all stages, screenshots, tables, and charts is provided in `report/project_report.pdf`.
