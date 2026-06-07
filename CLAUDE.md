# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**duze-dane** (Polish: "big data") is a university big data engineering project that processes geospatial and transit datasets using Hadoop MapReduce and Apache Spark, with an Angular 21 frontend for map visualization.

**Data sources:** GTFS (transit schedules via FTP), OSM (OpenStreetMap via HTTP), Real Estate (dane.gov.pl API)

## Commands

### Full Pipeline (everything from scratch)
```powershell
.\pipeline.ps1                                           # all steps
.\pipeline.ps1 -SkipInfrastructure -SkipAcquisition     # only processing + MapReduce
.\pipeline.ps1 -SkipMapReduce -SkipExperiments           # only infra + data
```
```bash
./pipeline.sh
./pipeline.sh --skip-infrastructure --skip-acquisition
```

### Infrastructure
```powershell
.\infrastructure\setup_hadoop.ps1        # start Hadoop/Spark cluster (default: .\data)
.\infrastructure\setup_nominatim.ps1     # start Nominatim geocoder (default: port 8083)
```
```bash
./infrastructure/setup_hadoop.sh
./infrastructure/setup_nominatim.sh
```

### MapReduce Jobs
```bash
./infrastructure/run_mapreduce.sh                        # all jobs (default)
./infrastructure/run_mapreduce.sh . ./data e1_1          # specific job
# job flags: e1_1 | e1_5 | e1_2 | e1_3 | e2 | e3 | exp_combiner | exp_reducers | all
```
```powershell
.\infrastructure\run_experiments.ps1                     # all experiments (default)
.\infrastructure\run_experiments.ps1 -Job exp_combiner
```

### Data Processing
```bash
python3 scripts/acquisition/deweloperuch.py              # download real estate data
python3 scripts/processing/extract.py <src> <out>        # normalize CSVs
python3 scripts/processing/geocode_real_estate.py <src> <out>  # geocode addresses
```

### Frontend (Angular 21)
```bash
cd deweloperka-frontend
npm install
npm start          # dev server at http://localhost:4200/
npm run build      # production build
npm test           # Vitest tests
```

### MapReduce (Java/Maven)
```bash
cd mapreduce
mvn clean package -DskipTests    # builds fat JAR in target/
```

## Repository Structure

```
pipeline.ps1 / pipeline.sh          ← master skrypt (all steps)

infrastructure/
  setup_hadoop.ps1 / .sh            ← start Hadoop/Spark Docker cluster
  setup_nominatim.ps1 / .sh         ← start Nominatim geocoder container
  run_mapreduce.sh                  ← MapReduce jobs (bash)
  run_experiments.ps1               ← experiments (PowerShell)
  spark_get_data.sh                 ← fetch results from HDFS

scripts/
  acquisition/
    deweloperuch.py                 ← multi-threaded real estate downloader
    merge_csv.sh                    ← merge + split CSVs into 128MB chunks
  processing/
    extract.py                      ← heterogeneous CSV normalization
    geocode_real_estate.py          ← address geocoding via Nominatim
    osm_to_csv.py                   ← PySpark: OSM GeoParquet → CSV
  config.py                         ← shared URLs and paths
  utils.py                          ← shared helpers

mapreduce/src/main/java/gtfs/       ← Java MapReduce jobs
deweloperka-frontend/               ← Angular 21 map frontend
data/
  gtfs/                             ← GTFS transit data
  deweloperka/                      ← raw real estate CSVs
  real_estate_merged/               ← normalized real estate CSVs
  real_estate_geocoded/             ← geocoded real estate CSVs (with lat/lon)
  osm/                              ← OSM GeoParquet + PBF file
```

## Architecture

### Data Pipeline Flow

```
scripts/acquisition/deweloperuch.py   (dane.gov.pl API)
  → scripts/processing/extract.py    (normalize CSVs)
  → scripts/processing/geocode_real_estate.py  (Nominatim geocoding)
  → infrastructure/setup_hadoop.ps1  (upload to HDFS)
  → infrastructure/run_mapreduce.sh  (E1.1 → E1.5 → E1.2 → E1.3 → E2 → E3)
  → deweloperka-frontend/            (Angular map visualization)
```

1. **Acquisition** (`scripts/acquisition/`): Multi-threaded downloads (20 workers) from GTFS FTP, OSM HTTP, and Real Estate API. JSON metadata tracking detects changes.

2. **ETL** (`scripts/processing/extract.py`): Reads real estate CSVs with encoding detection and regex-based column matching, outputs standardized CSV with semicolon delimiter.

3. **Geocoding** (`scripts/processing/geocode_real_estate.py`): Geocodes addresses via local Nominatim container (port 8083), adds lat/lon columns.

4. **MapReduce** (`mapreduce/src/main/java/gtfs/`):
   - `GtfsE11`: Map-side join of stop_times with trips/routes/calendar/stops → SequenceFile at `/user/hadoop/t1_1`
   - `GtfsE15`: Aggregates per-stop scores → StopInfo objects at `/user/hadoop/t1_5`
   - `OsmE12`: OSM POI data → GeoHash-keyed output at `/user/hadoop/t1_2`
   - `RealEstateE13`: Geocoded real estate → GeoHash-keyed output at `/user/hadoop/t1_3`
   - `E2Driver`: Reduce-side join of t1_5 + t1_2 + t1_3 → `/user/hadoop/t2`
   - `E3Driver`: Pearson correlation (transit score vs price) → `/user/hadoop/t3`

5. **Spark cluster**: Docker-based Spark 3.4.0 + Livy (REST job submission) + Jupyter Lab.

6. **Frontend** (`deweloperka-frontend/`): Angular 21 with standalone components and signal-based state. MapLibre GL renders vector tiles from openfreemap.org with 3D building extrusion.

### Key Java Classes (MapReduce)
- `GtfsE11.java` — Job 1 driver; map-side join implementation
- `GtfsE15.java` — Job 2 driver; aggregation pipeline
- `GtfsCombiner.java` — Combiner for E1.5 (preserves stopId key)
- `GtfsJoinMapper.java` / `GtfsAggregationMapper.java` / `GtfsAggregationReducer.java` — Hadoop M/R stages
- `StopTime.java` / `StopInfo.java` — Writable data models for HDFS serialization

### Infrastructure
- Hadoop 3.3.5 / Spark 3.4.0 via Docker Compose
- Nominatim 5.3 geocoder via Docker (port 8083), OSM PBF for Warsaw area
- HDFS replication factor: 3
- MapReduce jobs submitted with `yarn jar`

## Java/Maven Notes

- Java 8 target (`<source>1.8</source>`) — avoid Java 9+ APIs
- Fat JAR built with `maven-shade-plugin`; the shade JAR is what `yarn jar` executes
- Hadoop dependencies are `provided` scope (supplied by cluster at runtime)

## Python Notes

- `scripts/config.py` centralizes all data source URLs and paths
- `scripts/utils.py` has shared helpers used by both acquisition and extraction scripts
- Requires only `requests==2.33.1` (see `requirements.txt`)
