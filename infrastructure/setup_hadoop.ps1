# setup_hadoop.ps1 - uruchamia klaster Hadoop/Spark i uploaduje dane do HDFS
# Uzycie: .\setup_hadoop.ps1 [-DataDir <sciezka>]
# Przyklad: .\setup_hadoop.ps1

param(
    [string]$DataDir = "$PSScriptRoot\..\data"
)

$WORKSPACE_DIR = "D:\hadoop-workspace"
$CONTAINER     = "master"
$COMPOSE_DIR   = "$PSScriptRoot\..\spark\docker-script\hadoop"
$DATASETS      = @("gtfs", "osm", "real_estate_merged")

function info { param($msg) Write-Host "[INFO]  $msg" -ForegroundColor Green }
function warn { param($msg) Write-Host "[WARN]  $msg" -ForegroundColor Yellow }
function err  { param($msg) Write-Host "[ERROR] $msg" -ForegroundColor Red }

$DataDir = (Resolve-Path $DataDir -ErrorAction Stop).Path

New-Item -ItemType Directory -Force -Path $WORKSPACE_DIR | Out-Null

Set-Location $COMPOSE_DIR

$masterRunning = docker ps --format '{{.Names}}' 2>$null | Where-Object { $_ -eq "master" }
if ($masterRunning) {
    info "Klaster juz dziala - pomijam uruchamianie."
} else {
    info "Uruchamiam klaster (Hadoop 3.3.5 + Spark 3.4.0, 3 slavey)..."
    $wsUnix = $WORKSPACE_DIR -replace '^([A-Za-z]):\\', '/$1/' -replace '\\', '/'
    bash compose-up.sh 3.3.5 3.4.0 3 $wsUnix "$wsUnix/hadoop_data" "$wsUnix/hadoop_logs" "$wsUnix/spark_logs" 2>&1 | Out-Null

    if ($LASTEXITCODE -ne 0) {
        err "compose-up.sh zakonczyl sie bledem. Sprawdz logi Docker."
        exit 1
    }
}

info "Czekam na gotowos HDFS..."
$attempts = 0
while ($true) {
    docker exec $CONTAINER hdfs dfsadmin -report 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) { break }
    $attempts++
    if ($attempts -gt 60) {
        err "HDFS nie wystartowal po 5 minutach. Sprawdz: docker logs master"
        exit 1
    }
    Write-Host "  ... czekam ($attempts/60)" -ForegroundColor DarkGray
    Start-Sleep -Seconds 5
}

info "Czekam na wyjscie z trybu safe mode..."
docker exec $CONTAINER hdfs dfsadmin -safemode wait 2>&1 | Out-Null

info "HDFS gotowy. Uploaduje dane..."

foreach ($dataset in $DATASETS) {
    $localDir = Join-Path $DataDir $dataset

    if (-not (Test-Path $localDir)) {
        warn "Dataset '$dataset' nie istnieje w $DataDir - pomijam."
        continue
    }

    $files = Get-ChildItem $localDir -Recurse -File
    info "Uploaduje: $dataset ($($files.Count) plikow)..."

    foreach ($file in $files) {
        $relativePath = $file.FullName.Substring($DataDir.Length).Replace('\', '/')
        $hdfsPath = $relativePath
        $hdfsDir  = ($hdfsPath -replace '/[^/]+$', '')

        docker exec $CONTAINER hdfs dfs -mkdir -p $hdfsDir 2>&1 | Out-Null
        Get-Content $file.FullName -AsByteStream -Raw |
            docker exec -i $CONTAINER hdfs dfs -D dfs.replication=1 -put - $hdfsPath 2>&1 | Out-Null
    }
    info "  $dataset - gotowe."
}

info "Ustawiam wspolczynnik replikacji na 3..."
$existingDirs = @()
foreach ($dataset in $DATASETS) {
    docker exec $CONTAINER hdfs dfs -test -d "/$dataset" 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) { $existingDirs += "/$dataset" }
}
if ($existingDirs.Count -gt 0) {
    docker exec $CONTAINER hdfs dfs -setrep -R 3 ($existingDirs -join " ") 2>&1 | Out-Null
}

info "Gotowe! Klaster dziala."
info "YARN ResourceManager: http://localhost:8088"
info "HDFS NameNode:        http://localhost:9870"
