# Data Dictionary & Consumer Output Specification — USGS Earthquake Dataset (2023–2026)

## 1. Source Overview

- **Provider:** United States Geological Survey (USGS) — Earthquake Hazards Program
- **API Service:** FDSN Event Web Service (`fdsnws-event`, API v2.8.4)
- **Base Endpoint:** `https://earthquake.usgs.gov/fdsnws/event/1/query`
- **HTTP Method:** `GET`
- **Response Format:** GeoJSON (`FeatureCollection`, `application/geo+json`)
- **Official API Documentation:** <https://earthquake.usgs.gov/fdsnws/event/1/>
- **Official GeoJSON Field Reference:** <https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php>

---

## 2. Multi-Year Paginated Collection Configuration (2023–2026)

`src/api_client.py` collects all available earthquake events ($\text{mag} \ge 4.5$) for **2023**, **2024**, and **2025** completely, and **2026** from `2026-01-01T00:00:00Z` up to the exact UTC moment of collection (`2026-11-18T14:19:58Z`), using period-by-period and page-by-page pagination (`limit=2000`, `offset = 1 + (page - 1) * 2000`, `orderby=time-asc`).

| Period | `starttime` (UTC) | `endtime` (UTC) | Pages Fetched (`limit=2000`) | Collected Records |
|---|---|---|---:|---:|
| **2023** | `2023-01-01T00:00:00Z` | `2024-01-01T00:00:00Z` | 4 (`offset=1..6001`) | `7,643` |
| **2024** | `2024-01-01T00:00:00Z` | `2025-01-01T00:00:00Z` | 4 (`offset=1..6001`) | `6,397` |
| **2025** | `2025-01-01T00:00:00Z` | `2026-01-01T00:00:00Z` | 5 (`offset=1..8001`) | `8,559` |
| **2026** | `2026-01-01T00:00:00Z` | `2026-11-18T14:19:58Z` | 4 (`offset=1..6001`) | `7,032` |
| **Total** | `2023-01-01T00:00:00Z` | `2026-11-18T14:19:58Z` | **17 pages** | **29,631** |

---

## 3. Raw & Processed Files in the Pipeline

| Stage | File Path | Format | Description |
|---|---|---|---|
| **Stage 1 (API Collector)** | `data/raw/pages/usgs_<year>_page_<NN>.json` | GeoJSON | 17 unmodified raw GeoJSON page responses directly from USGS API |
| **Stage 1 (API Collector)** | `data/raw/usgs_earthquakes_2023_2026_records.json` | JSON Array | 29,631 flattened JSON records (14 fields each) ready for Kafka Producer |
| **Stage 1 (API Collector)** | `data/raw_data.csv` | CSV | Tabular snapshot of the 29,631 raw records |
| **Stage 1 (API Collector)** | `data/raw/usgs_earthquakes_2023_2026_metadata.json` | JSON | Full collection manifest (per-period counts, all 17 page URLs & offsets, ranges) |
| **Stage 2 (Kafka Producer)** | `data/kafka/producer_run_summary.json` | JSON | Producer execution summary (`29,631` messages published, offsets `0..29630`) |
| **Stage 2 (Kafka Consumer)** | `data/processed/consumed_earthquakes.jsonl` | JSON Lines | Append-only stream log of consumed messages including Kafka metadata (`_kafka_topic`, `_kafka_partition`, `_kafka_offset`, `_consumed_at_utc`) |
| **Stage 2 (Kafka Consumer)** | `data/processed/consumed_earthquakes.json` | JSON Array | Clean array of 29,631 consumed records (14 core fields) ready for Pandera validation (`src/validator.py`) |
| **Stage 2 (Kafka Consumer)** | `data/processed/consumed_earthquakes.csv` | CSV | Clean tabular CSV of 29,631 consumed records ready for `pandas` / `Pandera` |
| **Stage 2 (Kafka Verification)** | `data/kafka/offset_resume_verification.json` | JSON | Automated test report proving Consumer stop, Producer continuation, and Consumer offset resumption |

---

## 4. Field-Level Data Dictionary (Producer & Consumer Core Schema)

Each earthquake event consists of **14 core fields** published by `src/producer.py` and exported by `src/consumer.py` to `data/processed/consumed_earthquakes.json` and `data/processed/consumed_earthquakes.csv`:

| # | Field Name | Raw GeoJSON Path | Type | Category | Nullable in 29,631 Records | Observed Range / Distinct Values (2023–2026) | Description |
|---:|---|---|---|---|---|---:|---|---|
| 1 | `id` | `feature.id` | `str` | Identifier / Text | No (`0` nulls) | `29,631` unique IDs | Unique USGS event identifier (Primary Key for SQLite3) |
| 2 | `mag` | `properties.mag` | `float` | Numerical | No (`0` nulls) | `4.5` to `8.8` | Earthquake magnitude |
| 3 | `magType` | `properties.magType` | `str` | Categorical | No (`0` nulls) | `mb`, `md`, `ml`, `ml(texnet)`, `mw`, `mwb`, `mwc`, `mwp`, `mwr`, `mww` | Magnitude calculation algorithm |
| 4 | `net` | `properties.net` | `str` | Categorical | No (`0` nulls) | `ak`, `ci`, `hv`, `nc`, `nn`, `ok`, `pr`, `tx`, `us`, `uu`, `uw` | Contributing seismic network ID |
| 5 | `place` | `properties.place` | `str` | Categorical / Text | No (`0` nulls) | Free-text location descriptions | Human-readable geographic location of epicenter |
| 6 | `time` | `properties.time` | `int` | Date / Time | No (`0` nulls) | `2023-01-01T01:41:43.755Z` to `2026-11-18T11:10:57.037Z` | Origin time in Unix epoch milliseconds (UTC) |
| 7 | `updated` | `properties.updated` | `int` | Date / Time | No (`0` nulls) | Unix epoch milliseconds (UTC) | Most recent record update timestamp |
| 8 | `type` | `properties.type` | `str` | Categorical | No (`0` nulls) | `earthquake` (`29,631`) | Seismic event classification |
| 9 | `status` | `properties.status` | `str` | Categorical | No (`0` nulls) | `reviewed` (`29,631`) | Seismologist review status |
| 10 | `tsunami` | `properties.tsunami` | `int` | Numerical / Binary | No (`0` nulls) | `0` or `1` | `1` if linked to a tsunami warning/watch, else `0` |
| 11 | `sig` | `properties.sig` | `int` | Numerical | No (`0` nulls) | `312` to `2910` | Composite event significance score |
| 12 | `longitude` | `geometry.coordinates[0]` | `float` | Numerical | No (`0` nulls) | `-179.9997` to `179.9984` | Epicenter longitude in decimal degrees ($[-180, 180]$) |
| 13 | `latitude` | `geometry.coordinates[1]` | `float` | Numerical | No (`0` nulls) | `-65.8497` to `86.5939` | Epicenter latitude in decimal degrees ($[-90, 90]$) |
| 14 | `depth_km` | `geometry.coordinates[2]` | `float` | Numerical | No (`0` nulls) | `1.0` to `669.556` | Focal depth in kilometers |

---

## 5. Consumer Output Format Specification (Handoff to Validator & Cleaner)

`src/consumer.py` writes three output files in `data/processed/`:

1. **`data/processed/consumed_earthquakes.jsonl` (Stream Audit Log):**
   Each line is a JSON object containing the 14 earthquake fields **plus** 4 Kafka transport fields:
   - `_kafka_topic`: `"usgs_earthquakes"`
   - `_kafka_partition`: `0`
   - `_kafka_offset`: integer offset (`0 .. 29630`)
   - `_consumed_at_utc`: ISO-8601 UTC timestamp when the Consumer processed the message
2. **`data/processed/consumed_earthquakes.json` (JSON Array for Validator):**
   Standard JSON array of `29,631` objects containing the 14 core earthquake fields (`id` through `depth_km`), deduplicated by `id` while preserving stream order.
3. **`data/processed/consumed_earthquakes.csv` (Tabular CSV for `pandas` / `Pandera`):**
   UTF-8 CSV with header `id,mag,magType,net,place,time,updated,type,status,tsunami,sig,longitude,latitude,depth_km` and `29,631` rows. Can be loaded directly via `pd.read_csv("data/processed/consumed_earthquakes.csv")`.
