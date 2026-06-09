# deploy.ps1 - Uruchamia terraform apply i wgrywa dane do Databricks Unity Catalog Volume
# Uzycie: .\deploy.ps1 [-SkipTerraform] [-SkipUpload]
param(
    [switch]$SkipTerraform,
    [switch]$SkipUpload
)

$ErrorActionPreference = "Stop"
$ROOT      = Split-Path $PSScriptRoot -Parent
$TERRAFORM = $PSScriptRoot
$DATA      = "$ROOT\data"

function Write-Step { param($msg) Write-Host "`n==> $msg" -ForegroundColor Cyan }
function Write-Ok   { param($msg) Write-Host "    OK: $msg" -ForegroundColor Green }
function Write-Fail { param($msg) Write-Host "    FAIL: $msg" -ForegroundColor Red; exit 1 }

# -- 1. Terraform apply -------------------------------------------------------
if (-not $SkipTerraform) {
    Write-Step "Terraform apply --auto-approve"
    Push-Location $TERRAFORM
    try {
        terraform apply --auto-approve
        if ($LASTEXITCODE -ne 0) { Write-Fail "terraform apply failed (exit $LASTEXITCODE)" }
        Write-Ok "Infrastruktura postawiona"
    } finally {
        Pop-Location
    }
}

# -- Sprawdz databricks CLI ---------------------------------------------------
if (-not $SkipUpload) {
    $dbx = Get-Command databricks -ErrorAction SilentlyContinue
    if (-not $dbx) { Write-Fail "Nie znaleziono 'databricks' CLI. Zainstaluj: winget install Databricks.DatabricksCLI" }
}

# -- Funkcja do uploadu folderu -----------------------------------------------
function Upload-Folder {
    param(
        [string]$LocalDir,
        [string]$VolumePath,
        [string]$Label
    )
    if (-not (Test-Path $LocalDir)) {
        Write-Host "    SKIP: $LocalDir nie istnieje" -ForegroundColor Yellow
        return
    }
    Write-Step "Upload $Label -> $VolumePath"
    databricks fs cp --recursive --overwrite --concurrency 1 "$LocalDir" "dbfs:$VolumePath"
    if ($LASTEXITCODE -ne 0) { Write-Fail "Upload failed for $Label" }
    $total = (Get-ChildItem $LocalDir -Recurse -File).Count
    Write-Ok "$total files uploaded to $VolumePath"
}

# -- 2. Upload danych ---------------------------------------------------------
if (-not $SkipUpload) {
    $BASE = "/Volumes/workspace/duze_dane/data"

    Upload-Folder "$DATA\gtfs"                 "$BASE/gtfs"                 "GTFS"
    Upload-Folder "$DATA\osm"                  "$BASE/osm_csv"              "OSM (~5 GB, moze chwile potrwac)"
    Upload-Folder "$DATA\real_estate_geocoded" "$BASE/real_estate_geocoded" "Real Estate geocoded"
}


Write-Host "`n[DONE] Deploy complete." -ForegroundColor Green
