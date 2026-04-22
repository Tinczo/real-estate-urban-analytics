#!/usr/bin/env python3
import sys
import csv
from pathlib import Path

REQUIRED_COLS = [
    "Nazwa dewelopera",
    "Województwo lokalizacji przedsięwzięcia deweloperskiego lub zadania inwestycyjnego",
    "Powiat lokalizacji przedsięwzięcia deweloperskiego lub zadania inwestycyjnego",
    "Gmina lokalizacji przedsięwzięcia deweloperskiego lub zadania inwestycyjnego",
    "Miejscowość lokalizacji przedsięwzięcia deweloperskiego lub zadania inwestycyjnego",
    "Ulica lokalizacji przedsięwzięcia deweloperskiego lub zadania inwestycyjnego",
    "Nr nieruchomości lokalizacji przedsięwzięcia deweloperskiego lub zadania inwestycyjnego",
    "Kod pocztowy lokalizacji przedsięwzięcia deweloperskiego lub zadania inwestycyjnego",
    "Rodzaj nieruchomości: lokal mieszkalny, dom jednorodzinny ",
    "Data od której cena obowiązuje cena m 2 powierzchni użytkowej lokalu mieszkalnego / domu jednorodzinnego",
    "Cena m 2 powierzchni użytkowej lokalu mieszkalnego / domu jednorodzinnego [zł]",
    "Cena lokalu mieszkalnego lub domu jednorodzinnego będących przedmiotem umowy stanowiąca iloczyn ceny m2 oraz powierzchni [zł]",
    "Cena lokalu mieszkalnego lub domu jednorodzinnego uwzględniająca cenę lokalu stanowiącą iloczyn powierzchni oraz metrażu i innych składowych ceny, o których mowa w art. 19a ust. 1 pkt 1), 2) lub 3) [zł]",
]

def detect_encoding(filepath):
    encodings = ['utf-8-sig', 'utf-8', 'cp1250', 'iso-8859-2']
    for enc in encodings:
        try:
            with open(filepath, encoding=enc) as f:
                f.read(1024)
            return enc
        except UnicodeDecodeError:
            continue
    return 'utf-8'

def extract_file(csv_path, output_path, write_header=True):
    encoding = detect_encoding(csv_path)
    try:
        with open(csv_path, encoding=encoding, newline='') as f:
            # wykryj separator
            sample = f.read(1024)
            f.seek(0)
            sep = ';' if sample.count(';') > sample.count(',') else ','

            reader = csv.DictReader(f, delimiter=sep)
            if reader.fieldnames is None:
                return False, "pusty plik"

            # usun BOM z nazw kolumn
            fieldnames = [col.lstrip('\ufeff').strip() for col in reader.fieldnames]
            reader.fieldnames = fieldnames

            missing = [col for col in REQUIRED_COLS if col not in fieldnames]
            if missing:
                return False, f"brak kolumn: {missing}"

            with open(output_path, 'a', encoding='utf-8', newline='') as out:
                writer = csv.DictWriter(out, fieldnames=REQUIRED_COLS, delimiter=';', extrasaction='ignore')
                if write_header:
                    writer.writeheader()
                for row in reader:
                    writer.writerow({k: row.get(k, '').strip() for k in REQUIRED_COLS})

        return True, None
    except Exception as e:
        return False, str(e)


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

    developer_dirs = [d for d in source_dir.iterdir() if d.is_dir()]
    total = len(developer_dirs)
    processed = 0
    total_ok = 0
    total_skip = 0

    print(f"Przetwarzam {total} deweloperów...")

    for dev_dir in developer_dirs:
        ok, skip = process_developer(dev_dir, output_dir)
        total_ok += ok
        total_skip += skip
        if ok > 0:
            processed += 1
            print(f"[{processed}/{total}] {dev_dir.name}: {ok} plików OK, {skip} pominięto")

    print(f"\n=== Gotowe! Przetworzono {processed}/{total} deweloperów, "
          f"{total_ok} plików OK, {total_skip} pominięto ===")


if __name__ == "__main__":
    main()