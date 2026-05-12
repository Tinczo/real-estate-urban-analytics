"""
Geocoduje dane nieruchomości przez lokalny serwer Nominatim.
Wczytuje pliki CSV z data/real_estate_merged/, dodaje kolumny lat,lon
i zapisuje wyniki do data/real_estate_geocoded/.

Użycie: python3 geocode_real_estate.py <input_dir> <output_dir>
"""

import csv
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from urllib.parse import quote
from urllib.request import urlopen, Request
from urllib.error import URLError

NOMINATIM_URL = "http://localhost:8083/search"
MAX_WORKERS = 20
RETRY_DELAY = 1.0  # sekund między próbami przy błędzie
MAX_RETRIES = 3

# Kolumny oczekiwane w plikach wejściowych (z extract.py)
COL_NAME      = "Nazwa dewelopera"
COL_CITY      = "Miejscowość"
COL_STREET    = "Ulica"
COL_NUMBER    = "Nr"
COL_ZIP       = "Kod pocztowy"
COL_PRICE_M2  = "Cena m2"
COL_PRICE_TOT = "Cena całkowita"
COL_AREA      = "Powierzchnia"

cache: dict = {}
cache_lock = Lock()
cache_hits = 0
cache_miss = 0


def geocode_address(address: str) -> tuple:
    """
    Zwraca (lat, lon) dla podanego adresu lub (None, None) przy błędzie.
    Używa cache'u wątkowo-bezpiecznego.
    """
    global cache_hits, cache_miss

    with cache_lock:
        if address in cache:
            cache_hits += 1
            return cache[address]

    with cache_lock:
        cache_miss += 1

    url = f"{NOMINATIM_URL}?q={quote(address)}&format=json&limit=1&countrycodes=pl"
    req = Request(url, headers={"User-Agent": "BigDataProject/1.0"})

    for attempt in range(MAX_RETRIES):
        try:
            with urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())
                if data:
                    lat = float(data[0]["lat"])
                    lon = float(data[0]["lon"])
                    result = (lat, lon)
                else:
                    result = (None, None)
                with cache_lock:
                    cache[address] = result
                return result
        except (URLError, Exception):
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
            else:
                with cache_lock:
                    cache[address] = (None, None)
                return (None, None)


def build_address(row: dict) -> str:
    parts = []
    street = row.get(COL_STREET, "").strip()
    number = row.get(COL_NUMBER, "").strip()
    city   = row.get(COL_CITY, "").strip()
    zipcode = row.get(COL_ZIP, "").strip()

    if street:
        parts.append(f"{street} {number}".strip())
    if city:
        parts.append(city)
    if zipcode:
        parts.append(zipcode)

    return ", ".join(parts)


def process_file(input_path: str, output_path: str) -> int:
    """Przetwarza jeden plik CSV. Zwraca liczbę przetworzonych wierszy."""
    rows_in = []

    # Wykryj separator (średnik lub przecinek)
    with open(input_path, encoding="utf-8", errors="replace") as f:
        sample = f.read(2048)
        sep = ";" if sample.count(";") > sample.count(",") else ","

    with open(input_path, encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter=sep)
        for row in reader:
            rows_in.append(row)

    if not rows_in:
        return 0

    # Zbuduj unikalne adresy do geocodowania
    addresses = [build_address(r) for r in rows_in]

    # Równoległe geocodowanie
    results: list = [None] * len(addresses)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_idx = {executor.submit(geocode_address, addr): i
                         for i, addr in enumerate(addresses)}
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            results[idx] = future.result()

    # Zapisz wyniki
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fieldnames = [COL_NAME, COL_CITY, COL_STREET, COL_NUMBER, COL_ZIP,
                  COL_PRICE_M2, COL_PRICE_TOT, COL_AREA, "lat", "lon"]

    output_rows = []
    for row, (lat, lon) in zip(rows_in, results):
        if lat is None:
            continue
        row["lat"] = lat
        row["lon"] = lon
        output_rows.append(row)

    if not output_rows:
        return 0

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";", extrasaction="ignore")
        writer.writeheader()
        for row in output_rows:
            writer.writerow(row)

    return len(output_rows)


def main():
    if len(sys.argv) < 3:
        print("Użycie: geocode_real_estate.py <input_dir> <output_dir>")
        sys.exit(1)

    input_dir  = sys.argv[1]
    output_dir = sys.argv[2]
    os.makedirs(output_dir, exist_ok=True)

    csv_files = [f for f in os.listdir(input_dir)
                 if f.endswith(".csv") or f.startswith("part_")]

    if not csv_files:
        print(f"Brak plików CSV w: {input_dir}")
        sys.exit(1)

    total = 0
    for fname in sorted(csv_files):
        in_path  = os.path.join(input_dir, fname)
        base = fname.replace("part_", "geocoded_")
        if not base.endswith(".csv"):
            base += ".csv"
        out_path = os.path.join(output_dir, base)
        print(f"Przetwarzanie: {fname} ...", end=" ", flush=True)
        count = process_file(in_path, out_path)
        print(f"{count} wierszy")
        total += count

    print(f"\nGotowe. Razem: {total} rekordów z geocodowaniem.")
    print(f"Cache: {cache_hits} trafień, {cache_miss} zapytań do Nominatim.")


if __name__ == "__main__":
    main()
