# Real Estate Prices vs. Urban Surroundings

A university Big Data project (3-person team). The goal is to answer a simple question at scale:
**how strongly do apartment prices in Polish cities correlate with what surrounds them** — public transport,
shops, restaurants, green areas, schools and road noise?

The same analytical pipeline is implemented three times — as **Hadoop MapReduce (Java)**, as **PySpark**,
and as **Databricks jobs provisioned with Terraform** — so their performance and design trade-offs can be compared.

## Tech stack

| Area                 | Technologies                                                                                      |
| -------------------- | ------------------------------------------------------------------------------------------------- |
| Data acquisition     | Python (`requests`, FTP, multithreaded scraper of the [dane.gov.pl](https://dane.gov.pl) API)     |
| Geocoding            | Self-hosted [Nominatim](https://nominatim.org/) in Docker                                         |
| Storage              | HDFS, Parquet, CSV                                                                                |
| Processing           | Hadoop MapReduce 3 (Java, Maven), Apache Spark 3.4 / PySpark (DataFrames, Spark SQL, Pandas UDFs) |
| Local cluster        | Docker Compose — Hadoop master + 3 workers, YARN, Spark, Livy, JupyterLab                         |
| Cloud                | Databricks (serverless jobs, Unity Catalog volumes), Terraform                                    |
| Frontend (prototype) | Angular + MapLibre GL (3D map of Poland)                                                          |
| Automation           | Bash / PowerShell scripts (Linux and Windows)                                                     |

## Data sources

- **Real estate offers** — developers' price lists published on [dane.gov.pl](https://dane.gov.pl) (price per m², total price, area, city).
  A snapshot of the merged dataset is available [here](https://drive.google.com/file/d/1DZElFSzyLPhZwEdqleTTwV9syHw53y3v/view?usp=sharing).
- **Public transport** — GTFS feed of Warsaw's ZTM (`stops`, `trips`, `routes`, `calendar`, `stop_times`).
- **Points of interest** — OpenStreetMap extract for Poland (GeoParquet).

## Pipeline

```
 dane.gov.pl ─┐                ┌─ E1.1 GTFS join (map-side / broadcast) ─ E1.5 stop throughput (7–9 AM)─┐
 ZTM GTFS ────┼─ acquisition ──┼─ E1.2 OSM POIs → categories + weights ──────────────────────────────────┼─ E2 spatial join ─ E3 correlations
 OSM ─────────┘   + geocoding  └─ E1.3 real estate normalization ─────────────────────────────────────────┘   (geohash)          (per city)
```

1. **Acquisition** (`scripts/acquisition`) — downloads GTFS (FTP), OSM (HTTP) and all developer price-list datasets
   from the dane.gov.pl API in parallel, skipping unchanged files based on stored metadata.
2. **Preprocessing** (`scripts/processing`) — merges and normalizes the CSVs, geocodes addresses with Nominatim,
   and converts OSM data into a CSV of POIs with a category (`HANDEL`, `GASTRO`, `REKREACJA`, `EDUKACJA`, `DROGA`) and a weight.
3. **E1.x – extraction** — every record gets a 6-character **geohash**, which serves as the spatial join key.
   Transit stops are scored by the number of weekday morning-peak arrivals.
4. **E2 – indicators** — for every offer, all stops and POIs within 800 m (haversine distance) are used to compute:
   transit accessibility, retail, gastronomy, recreation and education indices, and a road-noise factor.
5. **E3 – analysis** — a Pearson correlation matrix between each indicator and price per m² / total price / area, per city.

### Experiments

The project also measures how engineering choices affect performance:

- MapReduce: with vs. without a **combiner**, different **numbers of reducers**,
- Spark: **broadcast join** on/off, number of **shuffle partitions** (1 / 10 / 50), **DataFrame API vs. Spark SQL**.

## Repository layout

```
scripts/            Python acquisition and preprocessing
mapreduce/          Hadoop MapReduce implementation (Java, Maven)
spark/              PySpark pipeline + Docker setup for Hadoop/Spark/Livy
jupyter-lab/        JupyterLab image for interactive work on the cluster
infrastructure/     Cluster setup, Nominatim, job runners and experiment scripts
terraform/          Databricks jobs, volumes and file uploads
deweloperka-frontend/  Angular map prototype
pipeline.sh / .ps1  End-to-end orchestration
```

## Running

### Local cluster (Docker)

```bash
pip install -r requirements.txt

# Full run: cluster + Nominatim, acquisition, preprocessing, pipeline, experiments
./pipeline.sh                          # MapReduce engine (default)
./pipeline.sh --engine=spark           # PySpark engine

# Skip stages that are already done
./pipeline.sh --skip-infrastructure --skip-acquisition --engine=spark
```

On Windows, use the `.ps1` equivalents. Once running, the YARN UI is at `http://localhost:8088` and the HDFS UI is at `http://localhost:9870`.

### Databricks (Terraform)

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars   # set databricks_host and databricks_token
terraform init
terraform apply
```

This creates the Unity Catalog volumes, uploads the scripts and defines jobs for the full pipeline (as a task DAG) and the experiments.
`deploy.ps1` runs `terraform apply` and then uploads the input data to the volumes with the Databricks CLI.

## Authors

Built by a team of three as part of a university Big Data course:

- Szymon Wieczorek - [@tinczo](https://github.com/tinczo)
- Mateusz Nasewicz - [@mateusznasewicz](https://github.com/mateusznasewicz)
- Szymon Jędrzejczak [@monczak](https://github.com/monczak)
