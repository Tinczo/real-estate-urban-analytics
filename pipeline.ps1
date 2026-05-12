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
    bash "$ROOT\infrastructure\run_mapreduce.sh" "$ROOT" "$DATA" all
    check
} else {
    warn "4/5 Pomijam MapReduce (-SkipMapReduce)"
}

# ── 5. Eksperymenty ──────────────────────────────────────────────────────────

if (-not $SkipExperiments) {
    step "5/5 Eksperymenty (combiner, reducery)"
    & "$ROOT\infrastructure\run_experiments.ps1" -Job all
    check
} else {
    warn "5/5 Pomijam eksperymenty (-SkipExperiments)"
}

# ── Podsumowanie ─────────────────────────────────────────────────────────────

$elapsed = [int]((Get-Date) - $t0).TotalSeconds
step "PIPELINE GOTOWY w ${elapsed}s"
info "YARN ResourceManager: http://localhost:8088"
info "HDFS NameNode:        http://localhost:9870"
