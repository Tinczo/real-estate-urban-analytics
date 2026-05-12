# run_experiments.ps1 - Eksperymenty MapReduce na klastrze Hadoop
# Uzycie: .\run_experiments.ps1 [-Job <flaga>] [-JarPath <sciezka>]
# Flagi: e1_1, e1_5, exp_combiner, exp_reducers, all
# Przyklad: .\run_experiments.ps1

param(
    [string]$Job = "all",
    [string]$JarPath = "$PSScriptRoot\..\mapreduce\target\mapreduce-1.0-SNAPSHOT.jar",
    [string]$Container = "master",
    [string]$LogDir = "$PSScriptRoot\..\data\logs"
)

$JAR_HDFS    = "/data/hadoop/mapreduce-1.0-SNAPSHOT.jar"
$T1_1        = "/user/hadoop/t1_1"
$T1_5        = "/user/hadoop/t1_5"
$LOG_FILE    = "$LogDir\experiments_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"
$timings     = [System.Collections.Generic.List[hashtable]]::new()

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

# ── Helpers ──────────────────────────────────────────────────────────────────

function log {
    param([string]$level, [string]$msg, [string]$color = "White")
    $ts   = Get-Date -Format "HH:mm:ss"
    $line = "[$ts] [$level] $msg"
    Write-Host $line -ForegroundColor $color
    Add-Content -Path $LOG_FILE -Value $line
}

function info  { param($m) log "INFO " $m "Cyan" }
function ok    { param($m) log "OK   " $m "Green" }
function warn  { param($m) log "WARN " $m "Yellow" }
function err   { param($m) log "ERROR" $m "Red" }

function separator {
    $line = "=" * 60
    Write-Host $line -ForegroundColor DarkGray
    Add-Content -Path $LOG_FILE -Value $line
}

# Wyciaga czas trwania joba z outputu YARN (linia "Job job_... completed")
function Get-YarnJobTime {
    param([string]$AppOutput)
    # Szukamy linii z map/reduce completions i liczymy czas ze znacznikow czasowych
    $lines = $AppOutput -split "`n"
    $startLine = $lines | Where-Object { $_ -match "Running job:" } | Select-Object -First 1
    $endLine   = $lines | Where-Object { $_ -match "completed successfully|completed with" } | Select-Object -First 1

    if ($startLine -and $endLine) {
        if ($startLine -match "(\d{2}:\d{2}:\d{2})," -and $endLine -match "(\d{2}:\d{2}:\d{2}),") {
            $t0 = [datetime]::ParseExact($Matches[1], "HH:mm:ss", $null)
            $endLine -match "(\d{2}:\d{2}:\d{2})," | Out-Null
            $t1 = [datetime]::ParseExact($Matches[1], "HH:mm:ss", $null)
            return [int]($t1 - $t0).TotalSeconds
        }
    }
    return -1
}

# Wyciaga countery z outputu YARN
function Get-YarnCounters {
    param([string]$AppOutput)
    $counters = @{}
    $AppOutput -split "`n" | Where-Object { $_ -match "Map input records|Map output records|Reduce input records|Reduce output records|CPU time" } | ForEach-Object {
        if ($_ -match "^\s+(.+?)=(\d+)") {
            $counters[$Matches[1].Trim()] = [long]$Matches[2]
        }
    }
    return $counters
}

# Uruchamia job YARN i zwraca (elapsed_s, output)
function Run-YarnJob {
    param([string]$Label, [string]$MainClass, [string]$JobArgs)

    separator
    info "START: $Label"
    info "Klasa: $MainClass $JobArgs"

    $hdfsArgs = $JobArgs.Trim() -replace '\s+', ' '
    $argList  = @("yarn", "jar", $JAR_HDFS, $MainClass) + ($hdfsArgs -split ' ' | Where-Object { $_ -ne '' })

    $t0 = Get-Date
    $output = & docker exec $Container $argList 2>&1 | Out-String
    $t1 = Get-Date
    $elapsed = [int]($t1 - $t0).TotalSeconds

    Add-Content -Path $LOG_FILE -Value $output

    $success = $output -match "completed successfully"
    if ($success) {
        ok "${Label}: SUKCES w ${elapsed}s"
    } else {
        err "${Label}: BLAD (${elapsed}s)"
        Write-Host ($output -split "`n" | Where-Object { $_ -match "ERROR|Exception|FAILED" } | Select-Object -Last 10) -ForegroundColor Red
    }

    # Parsuj countery
    $mapIn  = if ($output -match "Map input records=(\d+)")  { $Matches[1] } else { "-" }
    $mapOut = if ($output -match "Map output records=(\d+)") { $Matches[1] } else { "-" }
    $redOut = if ($output -match "Reduce output records=(\d+)") { $Matches[1] } else { "-" }
    $cpu    = if ($output -match "CPU time spent \(ms\)=(\d+)") { [int]([long]$Matches[1]/1000) } else { "-" }

    return @{
        Label    = $Label
        Success  = $success
        Elapsed  = $elapsed
        MapIn    = $mapIn
        MapOut   = $mapOut
        RedOut   = $redOut
        CpuSec   = $cpu
    }
}

function Hdfs-Remove { param($path) docker exec $Container bash -c "hdfs dfs -rm -r -skipTrash $path 2>/dev/null || true" | Out-Null }

# ── Jobs ─────────────────────────────────────────────────────────────────────

function Run-E1_1 {
    Hdfs-Remove $T1_1
    $r = Run-YarnJob "E1.1 GTFS Join" "gtfs.GtfsE11" "/gtfs/stop_times.txt /gtfs $T1_1"
    $timings.Add($r)
}

function Run-E1_5 {
    param([int]$NumReducers = 1, [bool]$WithCombiner = $false, [string]$Suffix = "")
    $outPath = if ($Suffix) { "${T1_5}_${Suffix}" } else { $T1_5 }
    $label   = if ($Suffix) { "E1.5 [$Suffix]" } else { "E1.5" }
    Hdfs-Remove $outPath
    $r = Run-YarnJob $label "gtfs.GtfsE15" "$T1_1 $outPath $NumReducers $($WithCombiner.ToString().ToLower())"
    $timings.Add($r)
    return $r
}

function Run-ExpCombiner {
    separator
    info "EKSPERYMENT B: Combiner ON vs OFF (1 reducer)"
    separator

    $rOff = Run-E1_5 -NumReducers 1 -WithCombiner $false -Suffix "comb_off"
    $rOn  = Run-E1_5 -NumReducers 1 -WithCombiner $true  -Suffix "comb_on"

    separator
    Write-Host ""
    Write-Host "  WYNIKI EKSPERYMENTU B: Combiner" -ForegroundColor Cyan
    Write-Host ""
    Write-Host ("  {0,-30} {1,8} {2,10}" -f "Wariant", "Czas [s]", "Status") -ForegroundColor White
    Write-Host ("  {0,-30} {1,8} {2,10}" -f ("-"*30), ("-"*8), ("-"*10)) -ForegroundColor DarkGray
    foreach ($r in @($rOff, $rOn)) {
        $status = if ($r.Success) { "OK" } else { "BLAD" }
        $color  = if ($r.Success) { "Green" } else { "Red" }
        Write-Host ("  {0,-30} {1,8} {2,10}" -f $r.Label, $r.Elapsed, $status) -ForegroundColor $color
    }

    if ($rOff.Success -and $rOn.Success) {
        $diff = $rOff.Elapsed - $rOn.Elapsed
        $pct  = if ($rOff.Elapsed -gt 0) { [math]::Round($diff * 100.0 / $rOff.Elapsed, 1) } else { 0 }
        Write-Host ""
        if ($diff -gt 0) {
            Write-Host "  Combiner przyspieszyl o ${diff}s (${pct}%)" -ForegroundColor Green
        } else {
            Write-Host "  Combiner nie dal przyspieszenia ($diff s)" -ForegroundColor Yellow
        }
    }
    Write-Host ""
}

function Run-ExpReducers {
    separator
    info "EKSPERYMENT C: Liczba reducerow (bez combinera)"
    separator

    $results = @()
    foreach ($n in 1, 2, 3) {
        $r = Run-E1_5 -NumReducers $n -WithCombiner $false -Suffix "r${n}"
        $results += $r
    }

    separator
    Write-Host ""
    Write-Host "  WYNIKI EKSPERYMENTU C: Liczba reducerow" -ForegroundColor Cyan
    Write-Host ""
    Write-Host ("  {0,-30} {1,8} {2,12} {3,10}" -f "Wariant", "Czas [s]", "Map output", "Status") -ForegroundColor White
    Write-Host ("  {0,-30} {1,8} {2,12} {3,10}" -f ("-"*30), ("-"*8), ("-"*12), ("-"*10)) -ForegroundColor DarkGray
    foreach ($r in $results) {
        $status = if ($r.Success) { "OK" } else { "BLAD" }
        $color  = if ($r.Success) { "Green" } else { "Red" }
        Write-Host ("  {0,-30} {1,8} {2,12} {3,10}" -f $r.Label, $r.Elapsed, $r.MapOut, $status) -ForegroundColor $color
    }
    Write-Host ""
}

# ── Podsumowanie końcowe ──────────────────────────────────────────────────────

function Show-Summary {
    separator
    Write-Host ""
    Write-Host "  PODSUMOWANIE WSZYSTKICH JOBOW" -ForegroundColor Cyan
    Write-Host ""
    Write-Host ("  {0,-30} {1,8} {2,12} {3,12} {4,10} {5,10}" -f `
        "Job", "Czas [s]", "Map input", "Map output", "CPU [s]", "Status") -ForegroundColor White
    Write-Host ("  {0,-30} {1,8} {2,12} {3,12} {4,10} {5,10}" -f `
        ("-"*30), ("-"*8), ("-"*12), ("-"*12), ("-"*10), ("-"*10)) -ForegroundColor DarkGray

    foreach ($r in $timings) {
        $status = if ($r.Success) { "OK" } else { "BLAD" }
        $color  = if ($r.Success) { "Green" } else { "Red" }
        Write-Host ("  {0,-30} {1,8} {2,12} {3,12} {4,10} {5,10}" -f `
            $r.Label, $r.Elapsed, $r.MapIn, $r.MapOut, $r.CpuSec, $status) -ForegroundColor $color
    }
    Write-Host ""
    Write-Host "  Log zapisany do: $LOG_FILE" -ForegroundColor DarkGray
    Write-Host ""
}

# ── Weryfikacja ───────────────────────────────────────────────────────────────

separator
info "Weryfikacja srodowiska..."

$hdfsOk = docker exec $Container hdfs dfsadmin -report 2>&1 | Select-String "Live datanodes"
if (-not $hdfsOk) { err "HDFS nie dziala! Uruchom klaster przez setup_hadoop.ps1"; exit 1 }
ok "HDFS dziala"

$gtfsOk = docker exec $Container bash -c "hdfs dfs -test -e /gtfs/stop_times.txt && echo ok" 2>&1
if ($gtfsOk -notmatch "ok") { err "Brak /gtfs/stop_times.txt na HDFS! Uruchom setup_hadoop.ps1"; exit 1 }
ok "Dane GTFS na HDFS"

$jarOk = docker exec $Container bash -c "test -f $JAR_HDFS && echo ok" 2>&1
if ($jarOk -notmatch "ok") {
    info "Kopiowanie JAR na master..."
    docker cp $JarPath "${Container}:${JAR_HDFS}"
    ok "JAR skopiowany"
} else {
    ok "JAR juz na masterze"
}

# Sprawdz czy t1_1 istnieje (potrzebne dla e1_5 i eksperymentow)
$t11Ok = docker exec $Container bash -c "hdfs dfs -test -d $T1_1 && echo ok" 2>&1

# ── Dispatch ─────────────────────────────────────────────────────────────────

separator
info "Uruchamianie: $Job"
separator

switch ($Job) {
    "e1_1" {
        Run-E1_1
    }
    "e1_5" {
        if ($t11Ok -notmatch "ok") { Run-E1_1 }
        Run-E1_5
    }
    "exp_combiner" {
        if ($t11Ok -notmatch "ok") { Run-E1_1 }
        Run-ExpCombiner
    }
    "exp_reducers" {
        if ($t11Ok -notmatch "ok") { Run-E1_1 }
        Run-ExpReducers
    }
    "all" {
        Run-E1_1
        Run-E1_5
        Run-ExpCombiner
        Run-ExpReducers
    }
    default {
        err "Nieznana flaga: '$Job'. Dostepne: e1_1, e1_5, exp_combiner, exp_reducers, all"
        exit 1
    }
}

Show-Summary
