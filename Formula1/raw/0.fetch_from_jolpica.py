# Databricks notebook source
# MAGIC %md
# MAGIC # Fetch F1 Data from Jolpica-F1 API → ADLS Raw Layer
# MAGIC
# MAGIC **API Base URL:** `https://api.jolpi.ca/ergast/f1/`
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Acts as a **connection test** — verifies the Databricks cluster can reach the Jolpica HTTPS endpoint
# MAGIC 2. Fetches all 8 F1 datasets and writes raw JSON to ADLS (`raw` container)
# MAGIC 3. Respects Jolpica community rate limits (1 request/second via `time.sleep(1)`)
# MAGIC
# MAGIC Run this **before** the ingestion notebooks to populate the raw layer.

# COMMAND ----------

import requests
import json
import time
from datetime import date

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("p_file_date", str(date.today()))
v_file_date = dbutils.widgets.get("p_file_date")

# Base endpoint — all calls use HTTPS
JOLPICA_BASE_URL = "https://api.jolpi.ca/ergast/f1"

print(f"File date  : {v_file_date}")
print(f"ADLS target: {raw_folder_path}/{v_file_date}/")
print(f"API source : {JOLPICA_BASE_URL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Connection Test
# MAGIC Ping the circuits endpoint. If this cell fails, check your cluster's outbound network rules.

# COMMAND ----------

test_url = f"{JOLPICA_BASE_URL}/circuits/?format=json&limit=1"
response = requests.get(test_url, timeout=10)

assert response.status_code == 200, \
    f"❌ CONNECTION FAILED — HTTP {response.status_code} from {test_url}"

print(f"✅ CONNECTION OK — HTTP {response.status_code} from {test_url}")
print(f"   Sample circuit: {response.json()['MRData']['CircuitTable']['Circuits'][0]['circuitId']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Fetch All Datasets and Write to ADLS

# COMMAND ----------

def fetch_all_pages(endpoint: str, record_key: str, page_size: int = 100) -> list:
    """
    Paginate through all pages of a Jolpica endpoint and return the full list of records.
    Respects the 1 req/sec community rate limit.
    """
    records = []
    offset = 0
    while True:
        url = f"{JOLPICA_BASE_URL}/{endpoint}/?format=json&limit={page_size}&offset={offset}"
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()["MRData"]
        total = int(data["total"])
        batch = data[record_key]
        # Handle Jolpica nested response keys (e.g. CircuitTable -> Circuits)
        # The record_key passed in is the inner list key (e.g. "Circuits")
        # data keys are e.g. {"CircuitTable": {"Circuits": [...]}}
        table_key = list(data.keys())[-1]          # e.g. "CircuitTable"
        inner_list = data[table_key][record_key]   # e.g. data["CircuitTable"]["Circuits"]
        records.extend(inner_list)
        offset += page_size
        if offset >= total:
            break
        time.sleep(1)  # Jolpica rate limit
    return records


def write_json_to_adls(records: list, dataset_name: str) -> str:
    """Write a list of dicts as newline-delimited JSON to ADLS raw container."""
    target_path = f"{raw_folder_path}/{v_file_date}/{dataset_name}/{dataset_name}.json"
    json_str = "\n".join(json.dumps(r) for r in records)
    # Use Databricks dbutils to write the file
    dbutils.fs.put(target_path, json_str, overwrite=True)
    return target_path

# COMMAND ----------

# MAGIC %md
# MAGIC ### Circuits

# COMMAND ----------

circuits_records = fetch_all_pages("circuits", "Circuits")
circuits_path = write_json_to_adls(circuits_records, "circuits")
print(f"✅ circuits  — {len(circuits_records):>5} records → {circuits_path}")
time.sleep(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Seasons / Races
# MAGIC Fetch races for a specific season (defaults to all races up to the current limit).

# COMMAND ----------

races_records = fetch_all_pages("races", "Races")
races_path = write_json_to_adls(races_records, "races")
print(f"✅ races     — {len(races_records):>5} records → {races_path}")
time.sleep(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Constructors

# COMMAND ----------

constructors_records = fetch_all_pages("constructors", "Constructors")
constructors_path = write_json_to_adls(constructors_records, "constructors")
print(f"✅ constructors — {len(constructors_records):>5} records → {constructors_path}")
time.sleep(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drivers

# COMMAND ----------

drivers_records = fetch_all_pages("drivers", "Drivers")
drivers_path = write_json_to_adls(drivers_records, "drivers")
print(f"✅ drivers   — {len(drivers_records):>5} records → {drivers_path}")
time.sleep(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Results

# COMMAND ----------

results_records = fetch_all_pages("results", "Results")
results_path = write_json_to_adls(results_records, "results")
print(f"✅ results   — {len(results_records):>5} records → {results_path}")
time.sleep(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pit Stops

# COMMAND ----------

pit_stops_records = fetch_all_pages("pitstops", "PitStops")
pit_stops_path = write_json_to_adls(pit_stops_records, "pit_stops")
print(f"✅ pit_stops — {len(pit_stops_records):>5} records → {pit_stops_path}")
time.sleep(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lap Times

# COMMAND ----------

lap_times_records = fetch_all_pages("laps", "Laps")
lap_times_path = write_json_to_adls(lap_times_records, "lap_times")
print(f"✅ lap_times — {len(lap_times_records):>5} records → {lap_times_path}")
time.sleep(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Qualifying

# COMMAND ----------

qualifying_records = fetch_all_pages("qualifying", "QualifyingResults")
qualifying_path = write_json_to_adls(qualifying_records, "qualifying")
print(f"✅ qualifying — {len(qualifying_records):>5} records → {qualifying_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Verify ADLS Write

# COMMAND ----------

print("\n📂 Files written to ADLS raw container:")
written_paths = [
    circuits_path, races_path, constructors_path, drivers_path,
    results_path, pit_stops_path, lap_times_path, qualifying_path
]
for p in written_paths:
    try:
        info = dbutils.fs.ls(p.rsplit("/", 1)[0])
        print(f"  ✅ {p.rsplit('/', 2)[-2]:>15}  — directory exists with {len(info)} file(s)")
    except Exception as e:
        print(f"  ❌ {p} — {e}")

# COMMAND ----------

dbutils.notebook.exit("Success — all Jolpica datasets fetched and written to ADLS raw layer")
