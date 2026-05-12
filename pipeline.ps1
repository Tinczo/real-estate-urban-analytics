# pipeline.ps1 - master skrypt: stawia infrastrukture, przetwarza dane, uruchamia potok MapReduce
# Uzycie: .\pipeline.ps1 [-SkipInfrastructure] [-SkipAcquisition] [-SkipProcessing] [-SkipMapReduce] [-SkipExperiments]
# Przyklad: .\pipeline.ps1
# Przyklad (tylko przetwarzanie, klaster juz stoi): .\pipeline.ps1 -SkipInfrastructure -SkipAcquisition

param(
    [switch]$SkipInfrastructure,
    [switch]$SkipAcquisition,
    [switch]$SkipProcessing,
    [switch]$SkipMapReduce,
    [switch]$SkipExperiments
)

$ROOT = $PSScriptRoot
$DATA = "$ROOT\data"

function info  { param($m) Write-Host "[INFO]  $m" -ForegroundColor Green }
function warn  { param($m) Write-Host "[WARN]  $m" -ForegroundColor Yellow }
function step  { param($m) Write-Host "`n===> $m" -ForegroundColor Cyan }
function check {
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[ERROR] Krok zakonczyl sie bledem (kod $LASTEXITCODE). Przerywam." -ForegroundColor Red
        exit $LASTEXITCODE
    }
}

$t0 = Get-Date
step "PIPELINE START ($(Get-Date -Format 'yyyy-MM-dd HH:mm:ss'))"

# ── 1. Infrastruktura ────────────────────────────────────────────────────────

if (-not $SkipInfrastructure) {
    step "1/5 Infrastruktura: Hadoop/Spark + Nominatim"
    info "Uruchamiam klaster Hadoop/Spark..."
    & "$ROOT\infrastructure\setup_hadoop.ps1" -DataDir $DATA
    check

    info "Uruchamiam Nominatim..."
    & "$ROOT\infrastructure\setup_nominatim.ps1"
    check
} else {
    warn "1/5 Pomijam infrastrukture (-SkipInfrastructure)"
}

# ── 2. Pobieranie danych ─────────────────────────────────────────────────────

if (-not $SkipAcquisition) {
    step "2/5 Pobieranie danych nieruchomosci"
    python "$ROOT\scripts\acquisition\deweloperuch.py"
    check
} else {
    warn "2/5 Pomijam pobieranie danych (-SkipAcquisition)"
}

# ── 3. Przetwarzanie lokalne ─────────────────────────────────────────────────

if (-not $SkipProcessing) {
    step "3/5 Przetwarzanie danych"
    info "Ekstrakcja i normalizacja CSV..."
    python "$ROOT\scripts\processing\extract.py" "$DATA\deweloperka\dane" "$DATA\real_estate_merged"
    check

    info "Geocodowanie adresow (Nominatim)..."
    python "$ROOT\scripts\processing\geocode_real_estate.py" "$DATA\real_estate_merged" "$DATA\real_estate_geocoded"
    check
} else {
    warn "3/5 Pomijam przetwarzanie (-SkipProcessing)"
}

# ── 4. MapReduce pipeline ────────────────────────────────────────────────────

if (-not $SkipMapReduce) {
    step "4/5 MapReduce pipeline (E1.1 -> E1.5 -> E1.2 -> E1.3 -> E2 -> E3)"

    # Upload brakujacych danych na HDFS
    $hdfsDatasets = [ordered]@{
        "gtfs"                 = "$DATA\gtfs"
        "osm"                  = "$DATA\osm"
        "real_estate_geocoded" = "$DATA\real_estate_geocoded"
    }
    foreach ($hdfsDir in $hdfsDatasets.Keys) {
        $localDir = $hdfsDatasets[$hdfsDir]
        if (-not (Test-Path $localDir)) { continue }
        docker exec master hdfs dfs -test -d "/$hdfsDir" 2>&1 | Out-Null
        if ($LASTEXITCODE -ne 0) {
            $files = Get-ChildItem $localDir -Recurse -File
            $totalMB = [math]::Round(($files | Measure-Object Length -Sum).Sum / 1MB, 1)
            info "Uploaduje: $hdfsDir ($($files.Count) plikow, ${totalMB} MB)..."
            $i = 0
            foreach ($file in $files) {
                $i++
                $rel     = $file.FullName.Substring($DATA.Length).Replace('\', '/')
                $hdfsDir2 = ($rel -replace '/[^/]+$', '')
                $sizeMB  = [math]::Round($file.Length / 1MB, 1)
                Write-Host "  [$i/$($files.Count)] $($file.Name) (${sizeMB} MB)" -ForegroundColor DarkGray
                docker exec master hdfs dfs -mkdir -p $hdfsDir2 2>&1 | Out-Null
                docker cp $file.FullName "master:/tmp/_upload_tmp" 2>&1 | Out-Null
                docker exec master hdfs dfs -D dfs.replication=1 -put -f /tmp/_upload_tmp $rel 2>&1 | Out-Null
            }
            docker exec master hdfs dfs -setrep -R 3 "/$hdfsDir" 2>&1 | Out-Null
            info "  $hdfsDir - gotowe."
        } else {
            info "  $hdfsDir - juz na HDFS, pomijam."
        }
    }

    # JAR
    $jar = Get-ChildItem "$ROOT\mapreduce\target" -Filter "mapreduce-*-SNAPSHOT.jar" -ErrorAction SilentlyContinue |
           Where-Object { $_.Name -notlike "original-*" } | Select-Object -First 1
    if (-not $jar) {
        info "Buduje JAR (mvn clean package)..."
        & mvn -f "$ROOT\mapreduce\pom.xml" clean package -DskipTests -q
        check
    }
    $jar = Get-ChildItem "$ROOT\mapreduce\target" -Filter "mapreduce-*-SNAPSHOT.jar" |
           Where-Object { $_.Name -notlike "original-*" } | Select-Object -First 1
    docker cp $jar.FullName "master:/data/hadoop/mapreduce-1.0-SNAPSHOT.jar" 2>&1 | Out-Null
    info "JAR skopiowany na mastera."

    $bashRoot = '/mnt/' + $ROOT[0].ToString().ToLower() + '/' + $ROOT.Substring(3) -replace '\\', '/'
    $bashData = '/mnt/' + $DATA[0].ToString().ToLower() + '/' + $DATA.Substring(3) -replace '\\', '/'
    bash -c "'$bashRoot/infrastructure/run_mapreduce.sh' '$bashRoot' '$bashData' all"
    check
} else {
    warn "4/5 Pomijam MapReduce (-SkipMapReduce)"
}

# ── 5. Eksperymenty ──────────────────────────────────────────────────────────

if (-not $SkipExperiments) {
    step "5/5 Eksperymenty (combiner, reducery)"
    $bashRoot = '/mnt/' + $ROOT[0].ToString().ToLower() + '/' + $ROOT.Substring(3) -replace '\\', '/'
    $bashData = '/mnt/' + $DATA[0].ToString().ToLower() + '/' + $DATA.Substring(3) -replace '\\', '/'
    bash -c "'$bashRoot/infrastructure/run_mapreduce.sh' '$bashRoot' '$bashData' exp_combiner"
    bash -c "'$bashRoot/infrastructure/run_mapreduce.sh' '$bashRoot' '$bashData' exp_reducers"
    check
} else {
    warn "5/5 Pomijam eksperymenty (-SkipExperiments)"
}

# ── Podsumowanie ─────────────────────────────────────────────────────────────

$elapsed = [int]((Get-Date) - $t0).TotalSeconds
step "PIPELINE GOTOWY w ${elapsed}s"
info "YARN ResourceManager: http://localhost:8088"
info "HDFS NameNode:        http://localhost:9870"
