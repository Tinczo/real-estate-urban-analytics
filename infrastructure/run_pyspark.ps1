param (
    [string]$ProjectDir = "",
    [string]$DataDir = "",
    [string]$JobFlag = "all"
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition

if ([string]::IsNullOrEmpty($ProjectDir)) {
    $ProjectDir = (Resolve-Path "$ScriptDir\..").ProviderPath
} else {
    $ProjectDir = (Resolve-Path $ProjectDir).ProviderPath
}

if ([string]::IsNullOrEmpty($DataDir)) {
    $DataDir = (Resolve-Path "$ProjectDir\data").ProviderPath
} else {
    $DataDir = (Resolve-Path $DataDir).ProviderPath
}

$Container = "spark"
$MasterContainer = "master"

# Skrypt python z pipeline'em
$PySparkScript = "/tmp/spark_pipeline.py"

# Sciezki HDFS
$HdfsGtfs = "/gtfs"
$HdfsOsmCsv = "/osm_csv"
$HdfsReGeocoded = "/real_estate_geocoded"
$HdfsT1_1 = "/user/hadoop/t1_1_py"
$HdfsT1_5 = "/user/hadoop/t1_5_py"
$HdfsT1_2 = "/user/hadoop/t1_2_py"
$HdfsT1_3 = "/user/hadoop/t1_3_py"
$HdfsT2 = "/user/hadoop/t2_py"
$HdfsT3 = "/user/hadoop/t3_py"

if (-not (Test-Path "$DataDir\logs")) {
    New-Item -ItemType Directory -Path "$DataDir\logs" | Out-Null
}
$DateStr = Get-Date -Format "yyyyMMdd_HHmmss"
$LogFile = "$DataDir\logs\pyspark_$DateStr.log"

function Log-Plain([string]$Msg) {
    # remove ansi codes if any, but since we echo plain text from ps we can just write
    $cleanMsg = $Msg -replace "`e\[[0-9;]*m", ""
    Add-Content -Path $LogFile -Value $cleanMsg
}

function Write-Info([string]$Msg) {
    Write-Host "[INFO]  $Msg" -ForegroundColor Green
    Log-Plain "[INFO]  $Msg"
}

function Write-Warn([string]$Msg) {
    Write-Host "[WARN]  $Msg" -ForegroundColor Yellow
    Log-Plain "[WARN]  $Msg"
}

function Write-ErrorMsg([string]$Msg) {
    Write-Host "[ERROR] $Msg" -ForegroundColor Red
    Log-Plain "[ERROR] $Msg"
    exit 1
}

function Write-Timing([string]$Msg) {
    Write-Host "[TIMING] $Msg" -ForegroundColor Yellow
    Log-Plain "[TIMING] $Msg"
}

function Invoke-Timed {
    param([string]$Label, [scriptblock]$Action)
    $t0 = Get-Date
    Write-Info "==> START: $Label ($($t0.ToString('HH:mm:ss')))"
    & $Action
    $t1 = Get-Date
    $elapsed = [math]::Round(($t1 - $t0).TotalSeconds)
    Write-Timing "$Label: ${elapsed}s"
}

function Hdfs-Rm([string]$Path) {
    docker exec $MasterContainer bash -c "hdfs dfs -rm -r -skipTrash $Path 2>/dev/null || true" | Out-Null
}

function Spark-Submit([string]$Stage, [string]$ExtraArgs) {
    $cmd = "/usr/local/spark/bin/spark-submit --master yarn --deploy-mode client --driver-memory 2g --executor-memory 2g $PySparkScript --stage $Stage $ExtraArgs"
    $proc = Start-Process -FilePath "docker" -ArgumentList "exec", $Container, "bash", "-c", $cmd -NoNewWindow -Wait -PassThru -RedirectStandardOutput "$DataDir\logs\temp_out.log" -RedirectStandardError "$DataDir\logs\temp_err.log"
    
    $out = Get-Content "$DataDir\logs\temp_out.log" -ErrorAction SilentlyContinue
    $err = Get-Content "$DataDir\logs\temp_err.log" -ErrorAction SilentlyContinue
    
    if ($out) { Write-Host ($out -join "`n"); Log-Plain ($out -join "`n") }
    if ($err) { Write-Host ($err -join "`n"); Log-Plain ($err -join "`n") }
    
    if ($proc.ExitCode -ne 0) {
        Write-ErrorMsg "PySpark Job zakonczyl sie bledem dla: $Stage"
    }
}

# ── Weryfikacja srodowiska ────────────────────────────────────────────────────

$sparkRunning = docker ps --format '{{.Names}}' | Select-String "^${Container}$"
if (-not $sparkRunning) { Write-ErrorMsg "Kontener '$Container' nie dziala. Uruchom klaster przez start.ps1" }

$masterRunning = docker ps --format '{{.Names}}' | Select-String "^${MasterContainer}$"
if (-not $masterRunning) { Write-ErrorMsg "Kontener '$MasterContainer' nie dziala. Uruchom klaster przez start.ps1" }

Write-Info "Kopiowanie spark_pipeline.py do kontenera spark..."
docker cp "$ProjectDir\spark\src\spark_pipeline.py" "${Container}:${PySparkScript}"

# ── Joby ─────────────────────────────────────────────────────────────────────

function Run-E1-1 {
    $check = docker exec $MasterContainer bash -c "hdfs dfs -test -e $HdfsGtfs/stop_times.txt && echo 1 || echo 0"
    if ($check.Trim() -eq "0") { Write-ErrorMsg "Brak danych GTFS na HDFS ($HdfsGtfs/stop_times.txt). Uruchom start.ps1" }
    
    Hdfs-Rm $HdfsT1_1
    Spark-Submit "e1_1" "--stop_times $HdfsGtfs/stop_times.txt --gtfs_cache $HdfsGtfs --t1_1 $HdfsT1_1"
}

function Run-E1-5 {
    Hdfs-Rm $HdfsT1_5
    Spark-Submit "e1_5" "--t1_1 $HdfsT1_1 --t1_5 $HdfsT1_5"
}

function Run-E1-2 {
    Hdfs-Rm $HdfsT1_2
    Spark-Submit "e1_2" "--osm_csv $HdfsOsmCsv --t1_2 $HdfsT1_2"
}

function Run-E1-3 {
    $check = docker exec $MasterContainer bash -c "hdfs dfs -test -d $HdfsReGeocoded && echo 1 || echo 0"
    if ($check.Trim() -eq "1") {
        Write-Info "==> Dane geocodowane juz sa na HDFS ($HdfsReGeocoded) - pomijam geocoding i upload."
    } else {
        Write-Info "==> Geocoding nieruchomosci (Nominatim)..."
        python3 "$ProjectDir\scripts\processing\geocode_real_estate.py" "$DataDir\real_estate_merged" "$DataDir\real_estate_geocoded"
        
        Write-Info "==> Upload danych geocodowanych do HDFS..."
        $files = Get-ChildItem -Path "$DataDir\real_estate_geocoded" -File -Recurse
        foreach ($f in $files) {
            $relPath = $f.FullName.Substring((Resolve-Path "$DataDir\real_estate_geocoded").ProviderPath.Length + 1).Replace('\', '/')
            # Using docker cp instead of piping cat into docker exec because powershell piping to native exes can corrupt binary data/newlines
            $tmpHdfs = "$HdfsReGeocoded/$relPath"
            docker exec $MasterContainer bash -c "mkdir -p `$(dirname $tmpHdfs)"
            docker cp $f.FullName "${MasterContainer}:/tmp/upload.csv"
            docker exec $MasterContainer bash -c "hdfs dfs -D dfs.replication=1 -put -f /tmp/upload.csv $tmpHdfs"
        }
        docker exec $MasterContainer bash -c "hdfs dfs -setrep -R 3 $HdfsReGeocoded" | Out-Null
    }
    
    Hdfs-Rm $HdfsT1_3
    Spark-Submit "e1_3" "--re_geocoded $HdfsReGeocoded --t1_3 $HdfsT1_3"
}

function Run-E2 {
    Hdfs-Rm $HdfsT2
    Spark-Submit "e2" "--t1_5 $HdfsT1_5 --t1_2 $HdfsT1_2 --t1_3 $HdfsT1_3 --t2 $HdfsT2"
}

function Run-E3 {
    Hdfs-Rm $HdfsT3
    Spark-Submit "e3" "--t2 $HdfsT2 --t3 $HdfsT3"
}

# ── Dispatch ──────────────────────────────────────────────────────────────────

Write-Info "Uruchamianie: $JobFlag"

switch ($JobFlag) {
    "e1_1" { Invoke-Timed "E1.1" { Run-E1-1 } }
    "e1_5" { Invoke-Timed "E1.5" { Run-E1-5 } }
    "e1_2" { Invoke-Timed "E1.2" { Run-E1-2 } }
    "e1_3" { Invoke-Timed "E1.3" { Run-E1-3 } }
    "e2"   { Invoke-Timed "E2"   { Run-E2 } }
    "e3"   { Invoke-Timed "E3"   { Run-E3 } }
    "all"  {
        Invoke-Timed "E1.1" { Run-E1-1 }
        Invoke-Timed "E1.5" { Run-E1-5 }
        Invoke-Timed "E1.2" { Run-E1-2 }
        Invoke-Timed "E1.3" { Run-E1-3 }
        Invoke-Timed "E2"   { Run-E2 }
        Invoke-Timed "E3"   { Run-E3 }
    }
    default { Write-ErrorMsg "Nieznana flaga: '$JobFlag'. Dostepne: e1_1, e1_5, e1_2, e1_3, e2, e3, all" }
}

Write-Info "Gotowe. Log: $LogFile"
Write-Timing "Podsumowanie czasow:"
Select-String -Path $LogFile -Pattern "\[TIMING\]" | ForEach-Object { Write-Host $_.Line }
