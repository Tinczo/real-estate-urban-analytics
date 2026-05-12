# setup_nominatim.ps1 - uruchamia lokalny kontener Nominatim do geocodingu
# Uzycie: .\setup_nominatim.ps1 [-PbfPath <sciezka>] [-Port <port>]
# Przyklad: .\setup_nominatim.ps1

param(
    [string]$PbfPath = "$PSScriptRoot\..\data\warsaw.osm.pbf",
    [int]$Port = 8083
)

function info { param($msg) Write-Host "[INFO]  $msg" -ForegroundColor Green }
function warn { param($msg) Write-Host "[WARN]  $msg" -ForegroundColor Yellow }
function err  { param($msg) Write-Host "[ERROR] $msg" -ForegroundColor Red }

$PbfPath = (Resolve-Path $PbfPath -ErrorAction SilentlyContinue)?.Path
if (-not $PbfPath -or -not (Test-Path $PbfPath)) {
    err "Nie znaleziono pliku PBF: $PbfPath"
    err "Pobierz plik OSM PBF i umiesz go w data/ lub podaj sciezke przez -PbfPath"
    exit 1
}

info "Zatrzymuje stary kontener nominatim (jesli istnieje)..."
docker rm -f nominatim 2>&1 | Out-Null

info "Uruchamiam Nominatim z pliku: $PbfPath"
info "Port: $Port"

docker run -d `
    -e PBF_PATH=/nominatim/data/warszawa.osm.pbf `
    -v "${PbfPath}:/nominatim/data/warszawa.osm.pbf" `
    -p "${Port}:8080" `
    --name nominatim `
    mediagis/nominatim:5.3

if ($LASTEXITCODE -ne 0) {
    err "Nie udalo sie uruchomic kontenera Nominatim."
    exit 1
}

info "Kontener uruchomiony. Czekam az Nominatim bedzie gotowy..."

$attempts = 0
while ($true) {
    try {
        $resp = Invoke-WebRequest -Uri "http://localhost:${Port}/search?q=Warszawa&format=json&limit=1" `
                                  -TimeoutSec 3 -ErrorAction Stop
        if ($resp.StatusCode -eq 200) {
            info "Nominatim gotowy! http://localhost:${Port}/search"
            break
        }
    } catch {}

    $attempts++
    if ($attempts -gt 60) {
        warn "Nominatim nie odpowiedzial po 10 minutach. Sprawdz: docker logs nominatim"
        warn "Moze trwac dluzej przy pierwszym imporcie danych."
        exit 1
    }
    Write-Host "  ... czekam na import ($attempts/60, co 10s)" -ForegroundColor DarkGray
    Start-Sleep -Seconds 10
}
