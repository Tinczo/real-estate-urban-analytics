#!/bin/bash

SOURCE_DIR="$(realpath "$1")" 
OUTPUT_DIR="$(realpath "$2")"
OUTPUT_FILE="$OUTPUT_DIR/all_real_estate_merged.csv"

mkdir -p "$OUTPUT_DIR"

echo "Rozpoczynam łączenie wszystkich plików z $SOURCE_DIR do $OUTPUT_FILE..."

HEADER_ADDED=false

find "$SOURCE_DIR" -type f -name "*.csv" | while read -r CSV_FILE; do
    if [ "$HEADER_ADDED" = false ]; then
        cat "$CSV_FILE" > "$OUTPUT_FILE"
        HEADER_ADDED=true
    else
        tail -n +2 "$CSV_FILE" >> "$OUTPUT_FILE"
    fi
done
split -C 128M --numeric-suffixes "$OUTPUT_FILE" "$OUTPUT_DIR/part_"
rm $OUTPUT_FILE
echo "Gotowe! Wszystkie pliki zostały połączone, a nastepnie posplitowane po 128MB"