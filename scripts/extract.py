#!/usr/bin/env python3
"""
Wyciąga ujednolicone kolumny z heterogenicznych CSV nieruchomości.
Obsługuje trzy schematy dane.gov.pl:
  - Format A (stary, 27 kol., sep=,): adres jako jeden string w "Lokalizacja..."
  - Format B (nowy, 40+ kol., sep=,): adres jako osobne kolumny, cena brutto/m2
  - Format C (nowy, 25+ kol., sep=;): jak B ale średnikowy, "Powierzchni lokalu" (literówka)
Wyjście: CSV z separatorem ";" i kolumnami:
  Nazwa dewelopera;Miejscowość;Ulica;Nr;Kod pocztowy;Cena m2;Cena całkowita;Powierzchnia
"""

import sys
import csv
import re
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

# Kolumna lokalizacji w formacie A
FORMAT_A_LOCATION_COL = "Lokalizacja przedsięwzięcia deweloperskiego lub zadania inwestycyjnego"

# Sygnały rozpoznania formatu — wystarczy że jedna z tych kolumn istnieje
FORMAT_A_SIGNAL = "Cena m 2 powierzchni użytkowej lokalu mieszkalnego / domu jednorodzinnego [zł]"
FORMAT_BC_SIGNAL = "Cena brutto/m2 powierzchni produktu [zł]"

# Mapowania kolumn dla każdego formatu (None = brak kolumny, zostanie "")
FORMAT_A_MAP = {
    "Nazwa dewelopera": "Nazwa dewelopera",
    "Cena m2":          "Cena m 2 powierzchni użytkowej lokalu mieszkalnego / domu jednorodzinnego [zł]",
    "Cena całkowita":   "Cena lokalu mieszkalnego lub domu jednorodzinnego  stanowiąca iloczym ceny m2 oraz powierzchni",
    "Powierzchnia":     None,
}

FORMAT_BC_MAP = {
    "Nazwa dewelopera": "Nazwa dewelopera",
    "Miejscowość":      "Miejscowość lokalizacji przedsięwzięcia deweloperskiego lub zadania inwestycyjnego",
    "Ulica":            "Ulica lokalizacji przedsięwzięcia deweloperskiego lub zadania inwestycyjnego",
    "Nr":               "Nr nieruchomości lokalizacji przedsięwzięcia deweloperskiego lub zadania inwestycyjnego",
    "Kod pocztowy":     "Kod pocztowy lokalizacji przedsięwzięcia deweloperskiego lub zadania inwestycyjnego",
    "Cena m2":          "Cena brutto/m2 powierzchni produktu [zł]",
    "Cena całkowita":   "Cena brutto produktu [zł]",
    # Powierzchnia: dwie możliwe nazwy (z literówką i bez)
}
FORMAT_BC_AREA_COLS = ["Powierzchnia lokalu [m2]", "Powierzchni lokalu [m2]"]

RE_ZIPCODE = re.compile(r"\b(\d{2}-\d{3})\b")


def detect_encoding(filepath):
    for enc in ('utf-8-sig', 'utf-8', 'cp1250', 'iso-8859-2'):
        try:
            with open(filepath, encoding=enc) as f:
                f.read(1024)
            return enc
        except UnicodeDecodeError:
            continue
    return 'utf-8'


def parse_location_a(location_str: str) -> dict:
    s = location_str.strip()
    result = {"Ulica": "", "Nr": "", "Kod pocztowy": "", "Miejscowość": ""}

    zip_match = RE_ZIPCODE.search(s)
    if zip_match:
        result["Kod pocztowy"] = zip_match.group(1)
        after_zip = s[zip_match.end():].strip().lstrip(",").strip()
        result["Miejscowość"] = after_zip.split(",")[0].strip()

    parts = s.split(",")
    street_raw = parts[0].strip()
    street_raw = re.sub(r"^(ul\.|al\.|os\.|pl\.|aleja|ulica)\s*", "", street_raw, flags=re.IGNORECASE)

    num_match = re.search(r"\s+(\d+[a-zA-Z]?(/\d+)?)$", street_raw)
    if num_match:
        result["Nr"] = num_match.group(1)
        result["Ulica"] = street_raw[:num_match.start()].strip()
    else:
        result["Ulica"] = street_raw

    return result


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

            if FORMAT_A_SIGNAL in fset:
                fmt = 'A'
            elif FORMAT_BC_SIGNAL in fset:
                fmt = 'BC'
            else:
                return False, f"nieznany schemat ({len(fieldnames)} kol, sep='{sep}')"

            # Znajdź kolumnę powierzchni dla BC (obsługa literówki)
            area_col = None
            if fmt == 'BC':
                for candidate in FORMAT_BC_AREA_COLS:
                    if candidate in fset:
                        area_col = candidate
                        break

            rows_written = 0
            with open(output_path, 'a', encoding='utf-8', newline='') as out:
                writer = csv.DictWriter(out, fieldnames=OUTPUT_COLS, delimiter=';', extrasaction='ignore')
                if write_header:
                    writer.writeheader()

                for row in reader:
                    if fmt == 'A':
                        loc = parse_location_a(row.get(FORMAT_A_LOCATION_COL, ""))
                        out_row = {
                            "Nazwa dewelopera": row.get("Nazwa dewelopera", "").strip(),
                            "Miejscowość":      loc["Miejscowość"],
                            "Ulica":            loc["Ulica"],
                            "Nr":               loc["Nr"],
                            "Kod pocztowy":     loc["Kod pocztowy"],
                            "Cena m2":          row.get(FORMAT_A_MAP["Cena m2"], "").strip(),
                            "Cena całkowita":   row.get(FORMAT_A_MAP["Cena całkowita"], "").strip(),
                            "Powierzchnia":     "",
                        }
                    else:
                        out_row = {out: row.get(src, "").strip() for out, src in FORMAT_BC_MAP.items()}
                        out_row["Powierzchnia"] = row.get(area_col, "").strip() if area_col else ""

                    if not out_row["Cena m2"] and not out_row["Cena całkowita"]:
                        continue

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
