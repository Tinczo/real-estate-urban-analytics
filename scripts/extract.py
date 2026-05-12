#!/usr/bin/env python3
"""
Wyciąga ujednolicone kolumny z heterogenicznych CSV nieruchomości z dane.gov.pl.
Pliki mają różne schematy, separatory i nazwy kolumn — używamy dopasowania regex.

Wyjście: CSV z separatorem ";" i kolumnami:
  Nazwa dewelopera;Miejscowość;Ulica;Nr;Kod pocztowy;Cena m2;Cena całkowita;Powierzchnia
"""

import sys
import re
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

# ---------------------------------------------------------------------------
# Wzorce regex do wykrywania kolumn — skompilowane raz
# ---------------------------------------------------------------------------

# Miejscowość inwestycji (nie siedziba dewelopera, nie biuro sprzedaży)
RE_CITY = re.compile(
    r'(?i)miejscowo[sś][cć]?\s+(lokalizacji(\s+przedsi[eę]wzi[eę]cia|\s+inwestycji)?'
    r'|inwestycji|adresu\s+lokalu|adresu\s+siedziby|'
    r'adresu\s+lokalu[\s,]|po[łl]o[żz]enia|lokalu\s+sprzeda)',
    re.UNICODE
)

# Priorytet: lokalizacja przedsięwzięcia > lokalizacja inwestycji > adres lokalu > reszta
RE_CITY_LOKALIZACJA = re.compile(
    r'(?i)miejscowo[sś][cć]?\s+lokalizacji\s+przedsi[eę]wzi[eę]cia',
    re.UNICODE
)
RE_CITY_INWESTYCJI = re.compile(
    r'(?i)(miejscowo[sś][cć]?\s+(lokalizacji\s+inwestycji|inwestycji)'
    r'|lokalizacja\s+inwestycji\s*-\s*miejscowo[sś][cć]?'
    r'|lokalizacja\s+przedsi[eę]wzi[eę]cia.*miejscowo[sś][cć]?)',
    re.UNICODE
)
RE_CITY_LOKALU = re.compile(
    r'(?i)miejscowo[sś][cć]?\s+adresu\s+lokalu',
    re.UNICODE
)
# Simple bare "Miejscowość" as last resort (some simplified schemas)
RE_CITY_BARE = re.compile(r'(?i)^miejscowo[sś][cć]?$', re.UNICODE)

# Ulica
RE_STREET_LOKALIZACJA = re.compile(
    r'(?i)ulica\s+lokalizacji\s+przedsi[eę]wzi[eę]cia',
    re.UNICODE
)
RE_STREET_INWESTYCJI = re.compile(
    r'(?i)(ulica\s+(lokalizacji\s+inwestycji|inwestycji)'
    r'|lokalizacja\s+inwestycji\s*-\s*ulica'
    r'|lokalizacja\s+przedsi[eę]wzi[eę]cia.*ulica)',
    re.UNICODE
)
RE_STREET_LOKALU = re.compile(
    r'(?i)ulica\s+adresu\s+lokalu',
    re.UNICODE
)
RE_STREET_BARE = re.compile(r'(?i)^ulica$', re.UNICODE)

# Nr nieruchomości (budynku) — NIE nr lokalu mieszkania
RE_NR_LOKALIZACJA = re.compile(
    r'(?i)nr\s+nieruchomo[sś][cć]i\s+lokalizacji\s+przedsi[eę]wzi[eę]cia',
    re.UNICODE
)
RE_NR_INWESTYCJI = re.compile(
    r'(?i)(nr\s+nieruchomo[sś][cć]i\s+(lokalizacji\s+inwestycji|inwestycji)'
    r'|lokalizacja\s+inwestycji.*nr\s+nieruchomo)',
    re.UNICODE
)
RE_NR_LOKALU = re.compile(
    r'(?i)nr\s+nieruchomo[sś][cć]i\s+adresu\s+lokalu',
    re.UNICODE
)
RE_NR_BARE = re.compile(r'(?i)^nr\s+nieruchomo[sś][cć]i$', re.UNICODE)

# Kod pocztowy
RE_ZIP_LOKALIZACJA = re.compile(
    r'(?i)kod\s+pocztowy\s+lokalizacji\s+przedsi[eę]wzi[eę]cia',
    re.UNICODE
)
RE_ZIP_INWESTYCJI = re.compile(
    r'(?i)(kod\s+pocztowy\s+(lokalizacji\s+inwestycji|inwestycji)'
    r'|lokalizacja\s+inwestycji.*kod\s+pocztowy)',
    re.UNICODE
)
RE_ZIP_LOKALU = re.compile(
    r'(?i)kod\s+pocztowy\s+adresu\s+lokalu',
    re.UNICODE
)
RE_ZIP_BARE = re.compile(r'(?i)^kod\s+pocztowy$', re.UNICODE)

# Cena m2 — dopasowanie regex, wykluczamy: Data, Historia, m2*powierzchnia, "za m2 pomieszczenia"
RE_PRICE_M2 = re.compile(
    r'(?i)(cena\s+(m\s*2|m[²2?]|metr[u]?|1\s*m[²2]|za\s*m[²2]?)\b'
    r'|cena/m[²2]'
    r'|cena_m2'
    r'|price_per_m2'
    r'|CenaM2'
    r'|Cena_m2'
    r'|CENA\s+BRUTTO\s+ZA\s+[1M])',
    re.UNICODE
)
RE_PRICE_M2_EXCLUDE = re.compile(
    r'(?i)^(data|historia|od\s+kt[oó]rej|poprzedni|przed\s+zmian|wykończ|pomieszczen|przynale|'
    r'ogr[oó]d|garaz|gara[żz]|kom[oó]rk|parking|miejsce\s+postojowe|cena_brutto_pln$)',
    re.UNICODE
)

# Cena całkowita — iloczyn ceny m2 i powierzchni = cena lokalu
RE_PRICE_TOT = re.compile(
    r'(?i)(iloczyn\s+ceny\s*m[\s2²]\s+oraz\s+powierzchni'
    r'|iloczym\s+ceny\s*m2\s+oraz\s+powierzchni'
    r'|cena\s+lokalu\s+(mieszkalnego|b[eę]d[aą]cego|mieszkania)'
    r'|cena\s+brutto\s+produktu'
    r'|cena\s+nieruchomo[sś]ci$'
    r'|cena\s+ca[łl]kowita$'
    r'|cena_lokalu_mieszkalnego.*powierzchni'
    r'|cena\s+iloczyn'
    r'|Cena\s+(ca[łl]kowita|brutto)\s*\(PLN\)$'
    r'|cena\s+(brutto\s+)?produktu\s*\[z[łl]\])',
    re.UNICODE
)
RE_PRICE_TOT_EXCLUDE = re.compile(
    r'(?i)^(data|historia|od\s+kt[oó]rej|poprzedni|przed\s+zmian|m2|za\s*m|za\s+metr)',
    re.UNICODE
)

# Powierzchnia lokalu (użytkowa)
RE_AREA = re.compile(
    r'(?i)(^powierzchni[aą]?\s+lokalu(\s+\[m2\]|\s+\(m[²2]\)|\s+lub\s+domu)?$'
    r'|^powierzchni\s+lokalu(\s+\[m2\])?$'
    r'|^powierzchnia\s+\[m2\]$'
    r'|^powierzchnia\s+\(m[²2?]\)$'
    r'|^powierzchnia$'
    r'|^powierzchnia_m2$'
    r'|^powierzchnia_uzytkowa(_m2)?$'
    r'|^PowierzchniaUzytk(owaM2|owa)$'
    r'|^powierzchnia\s+lokalu\s+mieszkalnego\s+w\s+m(\s*2)?$'
    r'|^powierzchnia\s+lokalu\s+lub\s+domu\s+jednorodzinnego\s+\[m2\]$)',
    re.UNICODE
)


def detect_encoding(filepath):
    # cp852/ibm852 handles files exported from some Polish accounting systems
    for enc in ('utf-8-sig', 'utf-8', 'cp1250', 'iso-8859-2', 'cp852', 'ibm852', 'latin-1'):
        try:
            with open(filepath, encoding=enc) as f:
                raw = f.read(4096)
            # Prefer encodings that produce actual Polish characters
            if any(c in raw for c in 'łóśćżźąęńŁÓŚĆŻŹĄĘŃ'):
                return enc
        except UnicodeDecodeError:
            continue
    # Fallback: use latin-1 (never raises, accepts all byte values)
    return 'latin-1'


def norm(s):
    return s.strip().lstrip('﻿').strip('"').strip()


def pick(fieldnames, *patterns):
    """Zwraca pierwszą kolumnę pasującą do pierwszego pasującego wzorca (priorytetowe)."""
    for pat in patterns:
        for col in fieldnames:
            if pat.search(col):
                return col
    return None


def pick_filtered(fieldnames, pattern, exclude):
    """Zwraca najkrótszą kolumnę pasującą do wzorca (z filtrem wykluczenia)."""
    matches = [c for c in fieldnames if pattern.search(c) and not exclude.search(c)]
    if not matches:
        return None
    return min(matches, key=len)


def extract_file(csv_path, output_path, write_header=True):
    encoding = detect_encoding(csv_path)
    try:
        with open(csv_path, encoding=encoding, newline='') as f:
            sample = f.read(4096)
            f.seek(0)
            sep = ';' if sample.count(';') > sample.count(',') else ','

            reader = csv.DictReader(f, delimiter=sep)
            if reader.fieldnames is None:
                return False, "pusty plik"

            fieldnames = [norm(col) for col in reader.fieldnames]
            reader.fieldnames = fieldnames

            # --- Kolumny adresowe (priorytet: lokalizacja > adres lokalu > bare name) ---
            city_col   = pick(fieldnames, RE_CITY_LOKALIZACJA, RE_CITY_INWESTYCJI, RE_CITY_LOKALU, RE_CITY_BARE)
            street_col = pick(fieldnames, RE_STREET_LOKALIZACJA, RE_STREET_INWESTYCJI, RE_STREET_LOKALU, RE_STREET_BARE)
            nr_col     = pick(fieldnames, RE_NR_LOKALIZACJA, RE_NR_INWESTYCJI, RE_NR_LOKALU, RE_NR_BARE)
            zip_col    = pick(fieldnames, RE_ZIP_LOKALIZACJA, RE_ZIP_INWESTYCJI, RE_ZIP_LOKALU, RE_ZIP_BARE)

            # --- Cena m2 ---
            price_m2_col = pick_filtered(fieldnames, RE_PRICE_M2, RE_PRICE_M2_EXCLUDE)
            if not price_m2_col:
                return False, f"brak kolumny ceny m2 ({len(fieldnames)} kol)"

            # --- Cena całkowita ---
            price_tot_col = pick_filtered(fieldnames, RE_PRICE_TOT, RE_PRICE_TOT_EXCLUDE)
            if price_tot_col == price_m2_col:
                price_tot_col = None

            # --- Powierzchnia ---
            area_col = pick(fieldnames, RE_AREA)

            # --- Nazwa dewelopera ---
            dev_col = next((c for c in fieldnames if c == "Nazwa dewelopera"), None)

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
                        "Nazwa dewelopera": row.get(dev_col,    "").strip() if dev_col    else "",
                        "Miejscowość":      row.get(city_col,   "").strip() if city_col   else "",
                        "Ulica":            row.get(street_col, "").strip() if street_col else "",
                        "Nr":               row.get(nr_col,     "").strip() if nr_col     else "",
                        "Kod pocztowy":     row.get(zip_col,    "").strip() if zip_col    else "",
                        "Cena m2":          cena_m2,
                        "Cena całkowita":   cena_tot,
                        "Powierzchnia":     row.get(area_col,   "").strip() if area_col   else "",
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
