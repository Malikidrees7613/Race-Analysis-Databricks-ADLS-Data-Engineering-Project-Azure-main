# Race-Analysis-Databricks-ADLS-Data-Engineering-Project-Azure
Analyzed F1 race data (1950s–2021) using Databricks for transformations and Azure Data Lake Storage Gen2 for storage in fully automated Azure Data Factory pipelines.

> **v2 Update:** Migrated data source from the legacy Ergast API to the **[Jolpica-F1 API](https://api.jolpi.ca/ergast/f1/)** — a community-maintained HTTPS mirror with maintained uptime and rate-limit-safe ingestion.

![Data Source](https://github.com/user-attachments/assets/a6af8eec-a653-4009-aace-4b36d8669c1d)

---

## Project Overview

This project covers the entire journey from raw data to valuable insights that can be utilized by machine learning engineers, data scientists, and analysts to produce meaningful results.

### 𝗧𝗲𝗰𝗵𝗻𝗼𝗹𝗼𝗴𝗶𝗲𝘀 𝗨𝘀𝗲𝗱 👩🏻‍💻

| Technology | Purpose |
|---|---|
| **Databricks** | Data processing — raw ingestion to final transformations |
| **Azure** | Cloud resource management |
| **Azure Data Factory** | Pipeline orchestration and automation |
| **Azure Data Lake Storage Gen2** | Storage for JSON, CSV, Parquet, and Delta files |
| **Azure Key Vault** | Secure storage account key management |
| **PySpark** | Distributed data processing |
| **SQL / PySpark SQL** | Data querying and analytics |
| **Delta Lake** | ACID transactions, data reliability, lakehouse architecture |
| **Jolpica-F1 API** | Live F1 data source (`https://api.jolpi.ca/ergast/f1/`) |

---

## Medallion Architecture 🏗️

| Layer | ADLS Container | Format | Description |
|---|---|---|---|
| **Bronze / Raw** | `raw` | JSON / CSV | Data fetched directly from Jolpica-F1 API via `raw/0.fetch_from_jolpica.py` |
| **Silver / Processed** | `processed` | Delta | Cleaned, typed, renamed, and deduplicated by ingestion notebooks |
| **Gold / Presentation** | `presentation` | Delta | Joined, aggregated, analytics-ready tables |

---

## Folder Structure 📁

```
Formula1/
├── raw/
│   └── 0.fetch_from_jolpica.py       ← NEW: Fetches all datasets from Jolpica API → ADLS
├── Ingestion/
│   ├── 0.ingest_all_files.py         ← Master orchestrator (rate-limited)
│   ├── 1.ingest_circuits_file.py     ← Updated for Jolpica JSON schema
│   ├── 2.ingest_races_file.py
│   ├── 3.ingest_constructor_file.py
│   ├── 4.ingest_driver_file.py
│   ├── 5.ingest_results_file.py
│   ├── 6.ingest_pit_stops_file.py
│   ├── 7.ingest_lap_times_file.py
│   └── 8.ingest_qualifying_file.py
├── includes/
│   ├── configuration.py              ← ADLS path variables
│   └── common_functions.py          ← Shared PySpark helpers (merge, partition, etc.)
├── trans/                            ← Transformation notebooks
├── analysis/                         ← Analysis / presentation notebooks
├── setup/                            ← Database and schema setup
└── utils/                            ← Incremental load utilities
```

---

## Pipeline Execution Order 🚀

Run notebooks in this sequence for a full end-to-end load:

```
1. Formula1/raw/0.fetch_from_jolpica.py        → Fetch raw JSON from Jolpica API to ADLS
2. Formula1/Ingestion/0.ingest_all_files.py    → Orchestrates all 8 ingestion notebooks
3. Formula1/trans/                              → Apply Silver-layer transformations
4. Formula1/analysis/                           → Build Gold-layer presentation tables
```

---

## 𝗣𝗿𝗼𝗷𝗲𝗰𝘁 𝗪𝗼𝗿𝗸𝗳𝗹𝗼𝘄 🛠️

### 𝗗𝗮𝘁𝗮 𝗖𝗼𝗹𝗹𝗲𝗰𝘁𝗶𝗼𝗻 📦
Fetched structured and semi-structured data (CSV, single-line JSON, multi-line JSON) from the **Jolpica-F1 API** (`https://api.jolpi.ca/ergast/f1/`) — the community-maintained HTTPS mirror of the legacy Ergast API — and stored it in Azure Data Lake Storage Gen2.
![Screenshot (96)](https://github.com/user-attachments/assets/6699efa4-9605-46c1-acd2-e661ece738cb)


### 𝗗𝗮𝘁𝗮 𝗜𝗻𝗴𝗲𝘀𝘁𝗶𝗼𝗻 🔢
Connected Databricks to the storage location and ingested the data from ADLS to Databricks.
![Screenshot (97)](https://github.com/user-attachments/assets/e57a08be-a730-4e49-8577-4a96ca8ebb5e)


### 𝗗𝗮𝘁𝗮 𝗩𝗮𝗹𝗶𝗱𝗮𝘁𝗶𝗼𝗻 ✅
Created multiple notebooks in Databricks for validating and processing different files.
![Screenshot (98)](https://github.com/user-attachments/assets/77ae2408-af70-45db-a839-f386f3eba4af)


### 𝗗𝗮𝘁𝗮 𝗧𝗿𝗮𝗻𝘀𝗳𝗼𝗿𝗺𝗮𝘁𝗶𝗼𝗻 🔎
Applied transformations and stored the processed files in the transformation layer on Azure Data Lake Storage in Delta format.
![Screenshot (99)](https://github.com/user-attachments/assets/60ee253d-bfd1-49b5-a4fc-c9341cb3dd35)


### 𝗗𝗮𝘁𝗮 𝗣𝗿𝗼𝗰𝗲𝘀𝘀𝗶𝗻𝗴 ⚙️
Processed the transformed data using PySpark and PySpark SQL to extract meaningful results.
![Screenshot (100)](https://github.com/user-attachments/assets/e168ed81-afef-4cdb-8a0d-ea822ec85f31)


### 𝗗𝗮𝘁𝗮𝗯𝗮𝘀𝗲 𝗖𝗿𝗲𝗮𝘁𝗶𝗼𝗻 🛢️
Created a database on Azure Data Lake Storage to store the processed data in tables.
![Screenshot (101)](https://github.com/user-attachments/assets/1d2c614e-576e-46ba-9704-0d5c4304c88f)


### 𝗦𝘁𝗼𝗿𝗮𝗴𝗲 🗂️
Stored all tables in the processed layer (container) on Azure Data Lake Storage in 𝗗𝗲𝗹𝘁𝗮 𝗙𝗼𝗿𝗺𝗮𝘁.
![Screenshot (102)](https://github.com/user-attachments/assets/99016342-f799-4d11-ac48-d514581b24c0)


### 𝗣𝗿𝗲𝘀𝗲𝗻𝘁𝗮𝘁𝗶𝗼𝗻 𝗟𝗮𝘆𝗲𝗿 🎯
Developed a presentation layer using SQL to join different tables and provide visual analytics.
![Screenshot (91)](https://github.com/user-attachments/assets/7f938f7a-63d0-404d-ad5d-c8669a2e780e)


### 𝗜𝗻𝘁𝗲𝗴𝗿𝗮𝘁𝗶𝗼𝗻 𝘄𝗶𝘁𝗵 𝗣𝗼𝘄𝗲𝗿 𝗕𝗜 📊
Connected the presentation layer to Power BI, enabling data analysts to perform further analysis and visualization.
![Screenshot (103)](https://github.com/user-attachments/assets/cd28f728-5977-4874-93de-02af1c2aa303)

##### *P.S.: The connection with Power BI hasn't been completed yet.*

### 𝗔𝘂𝘁𝗼𝗺𝗮𝘁𝗶𝗼𝗻 🤖
Utilized 𝗔𝘇𝘂𝗿𝗲 𝗗𝗮𝘁𝗮 𝗙𝗮𝗰𝘁𝗼𝗿𝘆 𝘁𝗼 𝗰𝗿𝗲𝗮𝘁𝗲 𝗮 𝗽𝗶𝗽𝗲𝗹𝗶𝗻𝗲 that automates the entire process. When new data is added to the raw container, the pipeline is triggered automatically, processing the data through all the engineering steps.
![Screenshot (95)](https://github.com/user-attachments/assets/47dd0f0f-8f06-4eb3-9799-eb1d8dd42ace)

---

## API Migration: Ergast → Jolpica-F1 🔄

### Why Jolpica?
The legacy **Ergast API** (`http://ergast.com/api/f1`) was deprecated and shut down in 2024. **[Jolpica-F1](https://api.jolpi.ca/ergast/f1/)** is its fully compatible community-maintained HTTPS replacement with the same endpoint structure.

### What Changed

#### New Notebook: `raw/0.fetch_from_jolpica.py`
A dedicated fetch utility notebook was added to the `raw/` layer. It:
- **Tests connectivity** to `https://api.jolpi.ca/ergast/f1/` (asserts HTTP 200)
- **Paginates** through all 8 F1 datasets automatically
- **Respects Jolpica's community rate limit** via `time.sleep(1)` between requests
- **Writes newline-delimited JSON** to `abfss://raw@<storage>.dfs.core.windows.net/<date>/<dataset>/`

#### Rate Limiting: `Ingestion/0.ingest_all_files.py`
`time.sleep(1)` added between every `dbutils.notebook.run()` call (7 sleeps total) to prevent overwhelming the Jolpica community server.

#### Circuits Schema Update: `Ingestion/1.ingest_circuits_file.py`
The Jolpica API returns a structurally different JSON response for circuits:

| Field | Legacy Ergast CSV | Jolpica JSON |
|---|---|---|
| `circuitId` | `INT` (e.g. `1`) | `STRING` slug (e.g. `"adelaide"`) |
| `name` | top-level | → `circuitName` (top-level) |
| `location` | top-level `STRING` | → `Location.locality` (nested) |
| `country` | top-level | → `Location.country` (nested) |
| `lat` / `lng` | flat `DOUBLE` | → `Location.lat` / `Location.long` nested `STRING` → cast to `DOUBLE` |
| `circuitRef` | present | **Removed** (not in Jolpica) |
| `alt` (altitude) | `INT` | **Removed** (not in Jolpica) |
| File format read | `.csv` | `.json` (newline-delimited) |

> ⚠️ **Note:** `circuit_id` is now a **string slug** in the processed layer. Any downstream joins on this column must treat it as `STRING`.

---

# Thank You for Consideration :)
