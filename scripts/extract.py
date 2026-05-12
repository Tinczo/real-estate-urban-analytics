#!/usr/bin/env python3
"""
Wyciąga ujednolicone kolumny z heterogenicznych CSV nieruchomości z dane.gov.pl.
Wszystkie pliki mają osobne kolumny adresowe — różnią się tylko separatorem i nazwą
kolumny z ceną m2.

Wyjście: CSV z separatorem ";" i kolumnami:
  Nazwa dewelopera;Miejscowość;Ulica;Nr;Kod pocztowy;Cena m2;Cena całkowita;Powierzchnia
"""

import sys
import csv
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

OUTPUT_COLS = [
    "Nazwa dewelopera",
    "Miejscowość",
    "Ulica",
    "Nr",
    "Kod pocztowy",
    "Cena m2",
    "Cena całkowita",
    "Powierzchnia",
]

# Kolumny adresowe — stałe we wszystkich wariantach schematu
COL_CITY    = "Miejscowość lokalizacji przedsięwzięcia deweloperskiego lub zadania inwestycyjnego"
COL_STREET  = "Ulica lokalizacji przedsięwzięcia deweloperskiego lub zadania inwestycyjnego"
COL_NR      = "Nr nieruchomości lokalizacji przedsięwzięcia deweloperskiego lub zadania inwestycyjnego"
COL_ZIP     = "Kod pocztowy lokalizacji przedsięwzięcia deweloperskiego lub zadania inwestycyjnego"

# Możliwe nazwy kolumny ceny m2 (różne wersje API)
PRICE_M2_CANDIDATES = [
    "Cena m 2 powierzchni użytkowej lokalu mieszkalnego / domu jednorodzinnego [zł]",
    "Cena m² powierzchni użytkowej lokalu mieszkalnego / domu jednorodzinnego [zł]",
    "Cena brutto/m2 powierzchni produktu [zł]",
]

# Możliwe nazwy kolumny ceny całkowitej
PRICE_TOT_CANDIDATES = [
    "Cena lokalu mieszkalnego lub domu jednorodzinnego będących przedmiotem umowy stanowiąca iloczyn ceny m2 oraz powierzchni [zł]",
    "Cena lokalu mieszkalnego lub domu jednorodzinnego będących przedmiotem umowy stanowiąca iloczyn ceny m 2 oraz powierzchni [zł]",
    "Cena lokalu mieszkalnego lub domu jednorodzinnego  stanowiąca iloczym ceny m2 oraz powierzchni",
    "Cena brutto produktu [zł]",
]

# Możliwe nazwy kolumny powierzchni
AREA_CANDIDATES = [
    "Powierzchnia lokalu [m2]",
    "Powierzchni lokalu [m2]",
]


def detect_encoding(filepath):
    for enc in ('utf-8-sig', 'utf-8', 'cp1250', 'iso-8859-2'):
        try:
            with open(filepath, encoding=enc) as f:
                f.read(1024)
            return enc
        except UnicodeDecodeError:
            continue
    return 'utf-8'


def find_col(fset, candidates):
    for c in candidates:
        if c in fset:
            return c
    return None


def extract_file(csv_path, output_path, write_header=True):
    encoding = detect_encoding(csv_path)
    try:
        with open(csv_path, encoding=encoding, newline='') as f:
            sample = f.read(2048)
            f.seek(0)
            sep = ';' if sample.count(';') > sample.count(',') else ','

            reader = csv.DictReader(f, delimiter=sep)
            if reader.fieldnames is None:
                return False, "pusty plik"

            fieldnames = [col.lstrip('﻿').strip() for col in reader.fieldnames]
            reader.fieldnames = fieldnames
            fset = set(fieldnames)

            # Wymagane: przynajmniej jedna kolumna z ceną m2
            price_m2_col  = find_col(fset, PRICE_M2_CANDIDATES)
            if not price_m2_col:
                return False, f"brak kolumny ceny m2 ({len(fieldnames)} kol)"

            price_tot_col = find_col(fset, PRICE_TOT_CANDIDATES)
            area_col      = find_col(fset, AREA_CANDIDATES)

            rows_written = 0
            with open(output_path, 'a', encoding='utf-8', newline='') as out:
                writer = csv.DictWriter(out, fieldnames=OUTPUT_COLS, delimiter=';', extrasaction='ignore')
                if write_header:
                    writer.writeheader()

                for row in reader:
                    cena_m2  = row.get(price_m2_col, "").strip()
                    cena_tot = row.get(price_tot_col, "").strip() if price_tot_col else ""

                    if not cena_m2 and not cena_tot:
                        continue

                    out_row = {
                        "Nazwa dewelopera": row.get("Nazwa dewelopera", "").strip(),
                        "Miejscowość":      row.get(COL_CITY,   "").strip(),
                        "Ulica":            row.get(COL_STREET, "").strip(),
                        "Nr":               row.get(COL_NR,     "").strip(),
                        "Kod pocztowy":     row.get(COL_ZIP,    "").strip(),
                        "Cena m2":          cena_m2,
                        "Cena całkowita":   cena_tot,
                        "Powierzchnia":     row.get(area_col, "").strip() if area_col else "",
                    }
                    writer.writerow(out_row)
                    rows_written += 1

        return True, None
    except Exception as e:
        return False, str(e)


print_lock = Lock()


def process_developer(developer_dir, output_dir):
    developer_id = developer_dir.name
    output_file = output_dir / f"{developer_id}.csv"
    output_file.unlink(missing_ok=True)

    csv_files = list(developer_dir.glob("*.csv"))
    if not csv_files:
        return 0, 0

    ok_count = 0
    skip_count = 0
    header_written = False

    for csv_file in csv_files:
        success, reason = extract_file(csv_file, output_file, write_header=not header_written)
        if success:
            ok_count += 1
            header_written = True
        else:
            skip_count += 1

    if not header_written:
        output_file.unlink(missing_ok=True)

    return ok_count, skip_count


def main():
    if len(sys.argv) != 3:
        print("Użycie: python3 extract.py <SOURCE_DIR> <OUTPUT_DIR>")
        sys.exit(1)

    source_dir = Path(sys.argv[1])
    output_dir = Path(sys.argv[2])
    output_dir.mkdir(parents=True, exist_ok=True)

    developer_dirs = sorted([d for d in source_dir.iterdir() if d.is_dir()])
    total = len(developer_dirs)

    total_ok = 0
    total_skip = 0
    processed = 0
    done = 0

    print(f"Przetwarzam {total} deweloperów (16 wątków)...")

    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = {executor.submit(process_developer, d, output_dir): d for d in developer_dirs}
        for future in as_completed(futures):
            ok, skip = future.result()
            done += 1
            total_ok += ok
            total_skip += skip
            if ok > 0:
                processed += 1
            with print_lock:
                dev_name = futures[future].name
                status = f"{ok} OK" if ok > 0 else "pominięto"
                print(f"[{done}/{total}] {dev_name}: {status}")

    print(f"\n=== Gotowe! {processed}/{total} deweloperów z danymi, "
          f"{total_ok} plików OK, {total_skip} pominięto ===")


if __name__ == "__main__":
    main()
