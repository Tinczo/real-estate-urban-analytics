#!/bin/bash
# run_experiments.sh - Eksperymenty MapReduce na klastrze Hadoop
# Uzycie: ./run_experiments.sh [flaga] [jar_path]
# Flagi: e1_1, e1_5, exp_combiner, exp_reducers, all
# Przyklad: ./run_experiments.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
JOB="${1:-all}"
JAR_PATH="${2:-$SCRIPT_DIR/../mapreduce/target/mapreduce-1.0-SNAPSHOT.jar}"
CONTAINER="master"
LOG_DIR="$SCRIPT_DIR/../data/logs"

JAR_HDFS="/data/hadoop/mapreduce-1.0-SNAPSHOT.jar"
T1_1="/user/hadoop/t1_1"
T1_5="/user/hadoop/t1_5"
LOG_FILE="$LOG_DIR/experiments_$(date +%Y%m%d_%H%M%S).log"

mkdir -p "$LOG_DIR"

declare -a TIMINGS_LABEL
declare -a TIMINGS_SUCCESS
declare -a TIMINGS_ELAPSED
declare -a TIMINGS_MAPIN
declare -a TIMINGS_MAPOUT
declare -a TIMINGS_REDOUT
declare -a TIMINGS_CPU

log() {
    local level="$1"
    local msg="$2"
    local color="$3"
    local ts
    ts=$(date +"%H:%M:%S")
    local line="[$ts] [$level] $msg"
    echo -e "${color}${line}\033[0m"
    echo "$line" >> "$LOG_FILE"
}

info() { log "INFO " "$1" "\033[0;36m"; }
ok()   { log "OK   " "$1" "\033[0;32m"; }
warn() { log "WARN " "$1" "\033[1;33m"; }
err()  { log "ERROR" "$1" "\033[0;31m"; }

separator() {
    local line="============================================================"
    echo -e "\033[1;30m${line}\033[0m"
    echo "$line" >> "$LOG_FILE"
}

run_yarn_job() {
    local label="$1"
    local main_class="$2"
    local job_args="$3"

    separator
    info "START: $label"
    info "Klasa: $main_class $job_args"

    local t0 t1 elapsed success output mapIn mapOut redOut cpu cpuMs
    t0=$(date +%s)
    # run docker exec and capture output
    output=$(docker exec "$CONTAINER" yarn jar "$JAR_HDFS" $main_class $job_args 2>&1 || true)
    t1=$(date +%s)
    elapsed=$((t1 - t0))

    echo "$output" >> "$LOG_FILE"

    if echo "$output" | grep -q "completed successfully"; then
        success=1
        ok "${label}: SUKCES w ${elapsed}s"
    else
        success=0
        err "${label}: BLAD (${elapsed}s)"
        echo "$output" | grep -E "ERROR|Exception|FAILED" | tail -n 10 | while read -r l; do
            echo -e "\033[0;31m$l\033[0m"
        done
    fi

    mapIn=$(echo "$output" | grep "Map input records=" | head -n 1 | awk -F'=' '{print $2}' || echo "-")
    [ -z "$mapIn" ] && mapIn="-"
    mapOut=$(echo "$output" | grep "Map output records=" | head -n 1 | awk -F'=' '{print $2}' || echo "-")
    [ -z "$mapOut" ] && mapOut="-"
    redOut=$(echo "$output" | grep "Reduce output records=" | head -n 1 | awk -F'=' '{print $2}' || echo "-")
    [ -z "$redOut" ] && redOut="-"
    cpuMs=$(echo "$output" | grep "CPU time spent (ms)=" | head -n 1 | awk -F'=' '{print $2}' || echo "-")
    if [ -n "$cpuMs" ] && [ "$cpuMs" != "-" ]; then
        cpu=$((cpuMs / 1000))
    else
        cpu="-"
    fi

    TIMINGS_LABEL+=("$label")
    TIMINGS_SUCCESS+=("$success")
    TIMINGS_ELAPSED+=("$elapsed")
    TIMINGS_MAPIN+=("$mapIn")
    TIMINGS_MAPOUT+=("$mapOut")
    TIMINGS_REDOUT+=("$redOut")
    TIMINGS_CPU+=("$cpu")
}

hdfs_remove() {
    docker exec "$CONTAINER" bash -c "hdfs dfs -rm -r -skipTrash $1 2>/dev/null || true" >/dev/null 2>&1
}

run_e1_1() {
    hdfs_remove "$T1_1"
    run_yarn_job "E1.1 GTFS Join" "gtfs.GtfsE11" "/gtfs/stop_times.txt /gtfs $T1_1"
}

run_e1_5() {
    local num_reducers="${1:-1}"
    local with_combiner="${2:-false}"
    local suffix="$3"
    
    local out_path="$T1_5"
    local label="E1.5"
    if [ -n "$suffix" ]; then
        out_path="${T1_5}_${suffix}"
        label="E1.5 [$suffix]"
    fi
    
    hdfs_remove "$out_path"
    run_yarn_job "$label" "gtfs.GtfsE15" "$T1_1 $out_path $num_reducers $with_combiner"
}

run_exp_combiner() {
    separator
    info "EKSPERYMENT B: Combiner ON vs OFF (1 reducer)"
    separator

    local start_idx=${#TIMINGS_LABEL[@]}
    
    run_e1_5 1 false "comb_off"
    run_e1_5 1 true "comb_on"

    local off_idx=$start_idx
    local on_idx=$((start_idx + 1))

    separator
    echo
    echo -e "\033[0;36m  WYNIKI EKSPERYMENTU B: Combiner\033[0m"
    echo
    printf "\033[1;37m  %-30s %8s %10s\033[0m\n" "Wariant" "Czas [s]" "Status"
    printf "\033[1;30m  %-30s %8s %10s\033[0m\n" "------------------------------" "--------" "----------"
    
    for i in $off_idx $on_idx; do
        local st_str="BLAD"
        local color="\033[0;31m"
        if [ "${TIMINGS_SUCCESS[$i]}" -eq 1 ]; then
            st_str="OK"
            color="\033[0;32m"
        fi
        printf "${color}  %-30s %8s %10s\033[0m\n" "${TIMINGS_LABEL[$i]}" "${TIMINGS_ELAPSED[$i]}" "$st_str"
    done

    if [ "${TIMINGS_SUCCESS[$off_idx]}" -eq 1 ] && [ "${TIMINGS_SUCCESS[$on_idx]}" -eq 1 ]; then
        local diff=$((${TIMINGS_ELAPSED[$off_idx]} - ${TIMINGS_ELAPSED[$on_idx]}))
        echo
        if [ $diff -gt 0 ]; then
            local pct=0
            if [ ${TIMINGS_ELAPSED[$off_idx]} -gt 0 ]; then
                pct=$(awk "BEGIN {printf \"%.1f\", ($diff * 100.0) / ${TIMINGS_ELAPSED[$off_idx]}}")
            fi
            echo -e "\033[0;32m  Combiner przyspieszyl o ${diff}s (${pct}%)\033[0m"
        else
            echo -e "\033[1;33m  Combiner nie dal przyspieszenia ($diff s)\033[0m"
        fi
    fi
    echo
}

run_exp_reducers() {
    separator
    info "EKSPERYMENT C: Liczba reducerow (bez combinera)"
    separator

    local start_idx=${#TIMINGS_LABEL[@]}
    
    for n in 1 2 3; do
        run_e1_5 $n false "r${n}"
    done

    separator
    echo
    echo -e "\033[0;36m  WYNIKI EKSPERYMENTU C: Liczba reducerow\033[0m"
    echo
    printf "\033[1;37m  %-30s %8s %12s %10s\033[0m\n" "Wariant" "Czas [s]" "Map output" "Status"
    printf "\033[1;30m  %-30s %8s %12s %10s\033[0m\n" "------------------------------" "--------" "------------" "----------"
    
    for ((i=start_idx; i<start_idx+3; i++)); do
        local st_str="BLAD"
        local color="\033[0;31m"
        if [ "${TIMINGS_SUCCESS[$i]}" -eq 1 ]; then
            st_str="OK"
            color="\033[0;32m"
        fi
        printf "${color}  %-30s %8s %12s %10s\033[0m\n" "${TIMINGS_LABEL[$i]}" "${TIMINGS_ELAPSED[$i]}" "${TIMINGS_MAPOUT[$i]}" "$st_str"
    done
    echo
}

show_summary() {
    separator
    echo
    echo -e "\033[0;36m  PODSUMOWANIE WSZYSTKICH JOBOW\033[0m"
    echo
    printf "\033[1;37m  %-30s %8s %12s %12s %10s %10s\033[0m\n" "Job" "Czas [s]" "Map input" "Map output" "CPU [s]" "Status"
    printf "\033[1;30m  %-30s %8s %12s %12s %10s %10s\033[0m\n" "------------------------------" "--------" "------------" "------------" "----------" "----------"
    
    local n=${#TIMINGS_LABEL[@]}
    for ((i=0; i<n; i++)); do
        local st_str="BLAD"
        local color="\033[0;31m"
        if [ "${TIMINGS_SUCCESS[$i]}" -eq 1 ]; then
            st_str="OK"
            color="\033[0;32m"
        fi
        printf "${color}  %-30s %8s %12s %12s %10s %10s\033[0m\n" "${TIMINGS_LABEL[$i]}" "${TIMINGS_ELAPSED[$i]}" "${TIMINGS_MAPIN[$i]}" "${TIMINGS_MAPOUT[$i]}" "${TIMINGS_CPU[$i]}" "$st_str"
    done
    echo
    echo -e "\033[1;30m  Log zapisany do: $LOG_FILE\033[0m"
    echo
}

# ── Weryfikacja ───────────────────────────────────────────────────────────────

separator
info "Weryfikacja srodowiska..."

if ! docker exec "$CONTAINER" hdfs dfsadmin -report 2>&1 | grep -q "Live datanodes"; then
    err "HDFS nie dziala! Uruchom klaster przez setup_hadoop.sh"
    exit 1
fi
ok "HDFS dziala"

if ! docker exec "$CONTAINER" bash -c "hdfs dfs -test -e /gtfs/stop_times.txt" 2>/dev/null; then
    err "Brak /gtfs/stop_times.txt na HDFS! Uruchom setup_hadoop.sh"
    exit 1
fi
ok "Dane GTFS na HDFS"

if ! docker exec "$CONTAINER" bash -c "test -f $JAR_HDFS" 2>/dev/null; then
    info "Kopiowanie JAR na master..."
    docker cp "$JAR_PATH" "${CONTAINER}:${JAR_HDFS}"
    ok "JAR skopiowany"
else
    ok "JAR juz na masterze"
fi

t11Ok=0
if docker exec "$CONTAINER" bash -c "hdfs dfs -test -d $T1_1" 2>/dev/null; then
    t11Ok=1
fi

# ── Dispatch ─────────────────────────────────────────────────────────────────

separator
info "Uruchamianie: $JOB"
separator

case "$JOB" in
    "e1_1")
        run_e1_1
        ;;
    "e1_5")
        if [ $t11Ok -eq 0 ]; then run_e1_1; fi
        run_e1_5
        ;;
    "exp_combiner")
        if [ $t11Ok -eq 0 ]; then run_e1_1; fi
        run_exp_combiner
        ;;
    "exp_reducers")
        if [ $t11Ok -eq 0 ]; then run_e1_1; fi
        run_exp_reducers
        ;;
    "all")
        run_e1_1
        run_e1_5
        run_exp_combiner
        run_exp_reducers
        ;;
    *)
        err "Nieznana flaga: '$JOB'. Dostepne: e1_1, e1_5, exp_combiner, exp_reducers, all"
        exit 1
        ;;
esac

show_summary
