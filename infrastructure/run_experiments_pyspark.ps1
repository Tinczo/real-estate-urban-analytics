param (
    [string]$ProjectDir = "",
    [string]$DataDir = ""
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

$PySparkScript = "/tmp/spark_pipeline.py"
$HdfsGtfs = "/gtfs"
$HdfsT1_1 = "/user/hadoop/t1_1_py"
$HdfsT1_5 = "/user/hadoop/t1_5_py"

if (-not (Test-Path "$DataDir\logs")) {
    New-Item -ItemType Directory -Path "$DataDir\logs" | Out-Null
}
$DateStr = Get-Date -Format "yyyyMMdd_HHmmss"
$LogFile = "$DataDir\logs\pyspark_experiments_$DateStr.log"

function Log-Plain([string]$Msg) {
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
    docker exec $Container bash -c $cmd 2>&1 | ForEach-Object {
        Write-Host $_
        Log-Plain $_
    }
    
    if ($LASTEXITCODE -ne 0) {
        Write-ErrorMsg "PySpark Job zakonczyl sie bledem dla: $Stage"
    }
}

Write-Info "Weryfikacja srodowiska..."
$sparkRunning = docker ps --format '{{.Names}}' | Select-String "^${Container}$"
if (-not $sparkRunning) { Write-ErrorMsg "Kontener '$Container' nie dziala. Uruchom klaster przez start.ps1" }

$masterRunning = docker ps --format '{{.Names}}' | Select-String "^${MasterContainer}$"
if (-not $masterRunning) { Write-ErrorMsg "Kontener '$MasterContainer' nie dziala. Uruchom klaster przez start.ps1" }

Write-Info "Kopiowanie spark_pipeline.py do kontenera spark..."
docker cp "$ProjectDir\spark\src\spark_pipeline.py" "${Container}:${PySparkScript}"

# Przygotowanie danych bazowych (jesli brak)
$check = docker exec $MasterContainer bash -c "hdfs dfs -test -e $HdfsGtfs/stop_times.txt && echo 1 || echo 0"
if ($check.Trim() -eq "0") { Write-ErrorMsg "Brak danych GTFS na HDFS. Uruchom start.ps1" }

# EKSPERYMENT 1: Broadcast Join vs SortMerge Join
function Run-Exp-Joins {
    Write-Info "=== EKSPERYMENT 1: Broadcast Join vs SortMerge Join (E1.1) ==="
    
    # Z Broadcast
    Hdfs-Rm "${HdfsT1_1}_exp_broadcast"
    Invoke-Timed "E1.1_BROADCAST_ON" {
        Spark-Submit "e1_1" "--stop_times $HdfsGtfs/stop_times.txt --gtfs_cache $HdfsGtfs --t1_1 ${HdfsT1_1}_exp_broadcast"
    }

    # Bez Broadcast
    Hdfs-Rm "${HdfsT1_1}_exp_nobroadcast"
    Invoke-Timed "E1.1_BROADCAST_OFF" {
        Spark-Submit "e1_1" "--stop_times $HdfsGtfs/stop_times.txt --gtfs_cache $HdfsGtfs --t1_1 ${HdfsT1_1}_exp_nobroadcast --disable_broadcast"
    }

    Write-Info "=== PODSUMOWANIE EKSPERYMENTU 1 ==="
    Select-String -Path $LogFile -Pattern "TIMING.*BROADCAST" | ForEach-Object { Write-Host $_.Line }
}

# EKSPERYMENT 2: Liczba partycji shuffle
function Run-Exp-Partitions {
    Write-Info "=== EKSPERYMENT 2: Liczba partycji shuffle (E1.5) ==="
    
    # Upewnij sie, ze E1.1 bazowe istnieje
    $checkT11 = docker exec $MasterContainer bash -c "hdfs dfs -test -d $HdfsT1_1 && echo 1 || echo 0"
    if ($checkT11.Trim() -eq "0") {
        Write-Info "Brak t1_1 - uruchamiam E1.1 najpierw..."
        Invoke-Timed "E1.1_base" {
            Spark-Submit "e1_1" "--stop_times $HdfsGtfs/stop_times.txt --gtfs_cache $HdfsGtfs --t1_1 $HdfsT1_1"
        }
    }

    $partitions = @(1, 10, 50)
    foreach ($N in $partitions) {
        Hdfs-Rm "${HdfsT1_5}_exp_p${N}"
        Invoke-Timed "E1.5_PARTITIONS_${N}" {
            Spark-Submit "e1_5" "--t1_1 $HdfsT1_1 --t1_5 ${HdfsT1_5}_exp_p${N} --reducers $N"
        }
    }

    Write-Info "=== PODSUMOWANIE EKSPERYMENTU 2 ==="
    Select-String -Path $LogFile -Pattern "TIMING.*PARTITIONS" | ForEach-Object { Write-Host $_.Line }
}

Run-Exp-Joins
Run-Exp-Partitions

Write-Info "Eksperymenty zakonczone. Zobacz pelny log: $LogFile"
