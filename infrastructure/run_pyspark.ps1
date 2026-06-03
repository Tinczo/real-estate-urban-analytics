# run_pyspark.ps1 - uruchamia pipeline w PySpark na klastrze Hadoop/Spark
# Uzycie: .\run_pyspark.ps1 [sciezka_do_projektu] [sciezka_do_danych] [job_flag]
# Flagi: e1_1, e1_5, e1_2, e1_3, e2, e3, all
# Przyklad: .\run_pyspark.ps1

param(
    [string]$ProjectDir = "",
    [string]$DataDir    = "",
    [string]$JobFlag    = "all"
)

$SCRIPT_DIR   = $PSScriptRoot
$PROJECT_DIR  = if ($ProjectDir) { $ProjectDir } else { "$SCRIPT_DIR\.." }
$DATA         = if ($DataDir)    { $DataDir }    else { "$PROJECT_DIR\data" }
$CONTAINER    = "spark"
$MASTER       = "master"
$PYSPARK_SCRIPT = "/tmp/spark_pipeline.py"

$HDFS_GTFS         = "/gtfs"
$HDFS_OSM_CSV      = "/osm_csv"
$HDFS_RE_GEOCODED  = "/real_estate_geocoded"
$HDFS_T1_1 = "/user/hadoop/t1_1_py"
$HDFS_T1_5 = "/user/hadoop/t1_5_py"
$HDFS_T1_2 = "/user/hadoop/t1_2_py"
$HDFS_T1_3 = "/user/hadoop/t1_3_py"
$HDFS_T2   = "/user/hadoop/t2_py"
$HDFS_T3   = "/user/hadoop/t3_py"

New-Item -ItemType Directory -Force "$DATA\logs" | Out-Null
$LOG_FILE = "$DATA\logs\pyspark_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"

function info   { param($m) $msg = "[INFO]  $m"; Write-Host $msg -ForegroundColor Green;  Add-Content $LOG_FILE $msg }
function warn   { param($m) $msg = "[WARN]  $m"; Write-Host $msg -ForegroundColor Yellow; Add-Content $LOG_FILE $msg }
function timing { param($m) $msg = "[TIMING] $m"; Write-Host $msg -ForegroundColor Yellow; Add-Content $LOG_FILE $msg }
function error_ {
    param($m)
    $msg = "[ERROR] $m"
    Write-Host $msg -ForegroundColor Red
    Add-Content $LOG_FILE $msg
    exit 1
}

function Run-Timed {
    param([string]$Label, [scriptblock]$Block)
    $t0 = Get-Date
    info "==> START: $Label ($((Get-Date).ToString('HH:mm:ss')))"
    & $Block
    $elapsed = [int]((Get-Date) - $t0).TotalSeconds
    timing "${Label}: ${elapsed}s"
}

function Hdfs-Rm {
    param([string]$Path)
    docker exec $MASTER bash -c "hdfs dfs -rm -r -skipTrash $Path 2>/dev/null || true" | Out-Null
}

function Spark-Submit {
    param([string]$Stage, [string]$ExtraArgs = "")
    $cmd = "/usr/local/spark/bin/spark-submit --master yarn --deploy-mode client --driver-memory 2g --executor-memory 2g $PYSPARK_SCRIPT --stage $Stage $ExtraArgs"
    docker exec $CONTAINER bash -c $cmd 2>&1 | Tee-Object -Append -FilePath $LOG_FILE
    if ($LASTEXITCODE -ne 0) { error_ "PySpark Job zakonczyl sie bledem dla: $Stage" }
}

# ── Weryfikacja srodowiska ────────────────────────────────────────────────────

$running = docker ps --format '{{.Names}}'
if ($running -notcontains $CONTAINER) { error_ "Kontener '$CONTAINER' nie dziala. Uruchom klaster." }
if ($running -notcontains $MASTER)    { error_ "Kontener '$MASTER' nie dziala. Uruchom klaster." }

# Wyjdz z safe mode jesli aktywny
$smStatus = docker exec $MASTER hdfs dfsadmin -report 2>&1 | Select-String "Safe mode is ON"
if ($smStatus) {
    warn "HDFS w safe mode - wymuszam wyjscie..."
    docker exec $MASTER hdfs dfsadmin -safemode forceExit 2>&1 | Out-Null
    info "Safe mode wylaczony."
}

info "Kopiowanie spark_pipeline.py do kontenera spark..."
docker cp "$PROJECT_DIR\spark\src\spark_pipeline.py" "${CONTAINER}:${PYSPARK_SCRIPT}"

# ── Joby ─────────────────────────────────────────────────────────────────────

function Run-E1_1 {
    docker exec $MASTER bash -c "hdfs dfs -test -e $HDFS_GTFS/stop_times.txt" 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) { error_ "Brak danych GTFS na HDFS ($HDFS_GTFS/stop_times.txt). Uruchom start.sh" }
    Hdfs-Rm $HDFS_T1_1
    Spark-Submit "e1_1" "--stop_times $HDFS_GTFS/stop_times.txt --gtfs_cache $HDFS_GTFS --t1_1 $HDFS_T1_1"
}

function Run-E1_5 {
    Hdfs-Rm $HDFS_T1_5
    Spark-Submit "e1_5" "--t1_1 $HDFS_T1_1 --t1_5 $HDFS_T1_5"
}

function Run-E1_2 {
    $check = docker exec $MASTER bash -c "hdfs dfs -test -d $HDFS_OSM_CSV && echo 1 || echo 0"
    if ($check.Trim() -eq "1") {
        info "==> Dane OSM CSV juz sa na HDFS ($HDFS_OSM_CSV) - pomijam konwersje."
    } else {
        info "==> Konwersja OSM Parquet -> CSV (PySpark)..."
        docker cp "$PROJECT_DIR\scripts\processing\osm_to_csv.py" "${CONTAINER}:/tmp/osm_to_csv.py"
        $cmd = "/usr/local/spark/bin/spark-submit --master yarn --deploy-mode client /tmp/osm_to_csv.py"
        docker exec $CONTAINER bash -c $cmd 2>&1 | Tee-Object -Append -FilePath $LOG_FILE
        if ($LASTEXITCODE -ne 0) { error_ "Konwersja OSM zakonczyla sie bledem." }
    }

    Hdfs-Rm $HDFS_T1_2
    Spark-Submit "e1_2" "--osm_csv $HDFS_OSM_CSV --t1_2 $HDFS_T1_2"
}

function Run-E1_3 {
    docker exec $MASTER bash -c "hdfs dfs -test -d $HDFS_RE_GEOCODED" 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) {
        info "==> Dane geocodowane juz sa na HDFS ($HDFS_RE_GEOCODED) - pomijam geocoding i upload."
    } else {
        info "==> Geocoding nieruchomosci (Nominatim)..."
        python "$PROJECT_DIR\scripts\processing\geocode_real_estate.py" `
            "$DATA\real_estate_merged" `
            "$DATA\real_estate_geocoded"

        info "==> Upload danych geocodowanych do HDFS..."
        $files = Get-ChildItem "$DATA\real_estate_geocoded" -Recurse -File
        foreach ($f in $files) {
            $rel = $f.FullName.Substring("$DATA\real_estate_geocoded\".Length) -replace '\\', '/'
            Get-Content $f.FullName -Raw | docker exec -i $MASTER bash -c `
                "hdfs dfs -D dfs.replication=1 -put - $HDFS_RE_GEOCODED/$rel"
        }
        docker exec $MASTER bash -c "hdfs dfs -setrep -R 3 $HDFS_RE_GEOCODED" 2>&1 | Out-Null
    }

    Hdfs-Rm $HDFS_T1_3
    Spark-Submit "e1_3" "--re_geocoded $HDFS_RE_GEOCODED --t1_3 $HDFS_T1_3"
}

function Run-E2 {
    Hdfs-Rm $HDFS_T2
    Spark-Submit "e2" "--t1_5 $HDFS_T1_5 --t1_2 $HDFS_T1_2 --t1_3 $HDFS_T1_3 --t2 $HDFS_T2"
}

function Run-E3 {
    Hdfs-Rm $HDFS_T3
    Spark-Submit "e3" "--t2 $HDFS_T2 --t3 $HDFS_T3"
}

# ── Dispatch ──────────────────────────────────────────────────────────────────

info "Uruchamianie: $JobFlag"

switch ($JobFlag) {
    "e1_1" { Run-Timed "E1.1" { Run-E1_1 } }
    "e1_5" { Run-Timed "E1.5" { Run-E1_5 } }
    "e1_2" { Run-Timed "E1.2" { Run-E1_2 } }
    "e1_3" { Run-Timed "E1.3" { Run-E1_3 } }
    "e2"   { Run-Timed "E2"   { Run-E2   } }
    "e3"   { Run-Timed "E3"   { Run-E3   } }
    "all"  {
        Run-Timed "E1.1" { Run-E1_1 }
        Run-Timed "E1.5" { Run-E1_5 }
        Run-Timed "E1.2" { Run-E1_2 }
        Run-Timed "E1.3" { Run-E1_3 }
        Run-Timed "E2"   { Run-E2   }
        Run-Timed "E3"   { Run-E3   }
    }
    default { error_ "Nieznana flaga: '$JobFlag'. Dostepne: e1_1, e1_5, e1_2, e1_3, e2, e3, all" }
}

info "Gotowe. Log: $LOG_FILE"
timing "Podsumowanie czasow:"
Select-String "\[TIMING\]" $LOG_FILE | ForEach-Object { $_.Line }
