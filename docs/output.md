## Co zostało zrobione

Implementacja wszystkich zadeklarowanych etapów przetwarzania danych w modelu Hadoop MapReduce, zgodnie z wymaganiami etapu 7. Cel: ocena 5.0.

### Nowe joby MapReduce

| Job | Klasa | Wejście | Wyjście | Opis |
|-----|-------|---------|---------|------|
| E1.2 | `OsmE12` | OSM CSV (z PySpark) | `t1_2` (SequenceFile) | Ekstrakcja POI OSM, przypisanie kategorii i wagi |
| E1.3 | `RealEstateE13` | CSV z geocodingiem | `t1_3` (SequenceFile) | Normalizacja ofert nieruchomości + GEO_ID |
| E2 | `E2Driver` | t1_5 + t1_2 + t1_3 | `t2` (CSV) | Reduce-side Join po GEO_ID, obliczanie 6 wskaźników per oferta |
| E3 | `E3Driver` | t2 | `t3` (CSV) | Macierz korelacji Pearsona per miasto |

### Modyfikacje istniejących jobów

- **`GtfsE15`** - dodano obsługę parametrów `numReducers` i `withCombiner` (potrzebne do eksperymentów), bez zmian w logice

### Nowe skrypty Python

- **`scripts/osm_to_csv.py`** - konwertuje dane OSM z GeoParquet → CSV przez PySpark (prereq dla E1.2)
- **`scripts/geocode_real_estate.py`** - geocoduje adresy nieruchomości przez lokalny Nominatim, 20 wątków równolegle, cache wyników

### Eksperymenty (wymagane do oceny 5.0)

- **Eksperyment B:** Combiner włączony vs wyłączony na E1.5
- **Eksperyment C:** Liczba reducerów (1, 2, 3) na E1.5

### Rozszerzenie `run_mapreduce.sh`

Nowe flagi:

```
e1_2, e1_3, e2, e3          - uruchamiają konkretny job
exp_combiner                 - eksperyment B (2 uruchomienia E1.5)
exp_reducers                 - eksperyment C (3 uruchomienia E1.5)
all                          - cały pipeline od E1.1 do E3
```

Każdy job jest opakowany w `run_timed` - czasy zapisywane są do pliku `data/logs/mapreduce_YYYYMMDD_HHMMSS.log`. Logi YARN zbierane automatycznie po każdym jobie.

---

## Jak uruchomić

### Wymagania wstępne

1. Klaster Hadoop uruchomiony (`./start.sh <data_dir>`)
2. Dane GTFS, OSM i real estate wgrane do HDFS
3. Zbudowany fat JAR:

```bash
cd mapreduce
mvn clean package -DskipTests
```

4. Nominatim uruchomiony lokalnie na porcie 8080 (dla E1.3)

### Eksperymenty (zacznij od tego - nie potrzeba nowych danych)

```bash
# Eksperyment B: Combiner
./run_mapreduce.sh <project_dir> <data_dir> exp_combiner

# Eksperyment C: Liczba reducerów
./run_mapreduce.sh <project_dir> <data_dir> exp_reducers

# Wyniki (czasy) w logu:
grep TIMING data/logs/mapreduce_*.log
```

### Pełny pipeline

```bash
# Krok 0: Geocoding nieruchomości (lokalnie, przed startem klastra)
python3 scripts/geocode_real_estate.py data/real_estate_merged data/real_estate_geocoded
# Zostaw na noc - może trwać 1-2h przy dużym zbiorze

# Krok 1: E1.2 OSM (wymaga klastra Spark do konwersji Parquet)
./run_mapreduce.sh <project_dir> <data_dir> e1_2

# Krok 2: E1.3 nieruchomości
./run_mapreduce.sh <project_dir> <data_dir> e1_3

# Krok 3: E2 - główny join (wymaga t1_5 z wcześniejszego E1.5, t1_2 i t1_3)
./run_mapreduce.sh <project_dir> <data_dir> e2

# Krok 4: E3 - korelacje
./run_mapreduce.sh <project_dir> <data_dir> e3

# Albo wszystko naraz:
./run_mapreduce.sh <project_dir> <data_dir> all
```

---

## Jak sprawdzić czy działa

### E1.2 - OSM

```bash
docker exec master bash -c "hdfs dfs -ls /user/hadoop/t1_2"
# Powinno być: part-m-00000, part-m-00001, part-m-00002 + _SUCCESS
```

### E1.3 - nieruchomości

```bash
docker exec master bash -c "hdfs dfs -ls /user/hadoop/t1_3"
```

### E2 - wskaźniki per oferta

```bash
docker exec master bash -c "hdfs dfs -cat /user/hadoop/t2/part-r-00000 | head -5"
# Oczekiwany format:
# Warszawa|u3qbuv   1048592,u3qbuv,15504.58,846000.00,54.50,Warszawa,154.220000,12.871000,...
```

### E3 - korelacje Pearsona

```bash
docker exec master bash -c "hdfs dfs -cat /user/hadoop/t3/part-r-00000 | head -20"
# Oczekiwany format:
# Warszawa   wsk_przepustowosci,0.342100,-0.121300,0.087200
# Warszawa   ind_handlu,0.198400,...
```

### Eksperymenty - tabela do raportu

```bash
grep "\[TIMING\]" data/logs/mapreduce_*.log
# Przykład wyjścia:
# [TIMING] E1.5_combiner_OFF: 87s
# [TIMING] E1.5_combiner_ON: 61s
# [TIMING] E1.5_reducers_1: 87s
# [TIMING] E1.5_reducers_2: 74s
# [TIMING] E1.5_reducers_3: 68s
```

### Countery Hadoop (logi statusu)

W logach YARN po każdym jobie widoczne są countery:

```
OSM_E12: RECORDS_PROCESSED, RECORDS_SKIPPED_*
E2: CELLS_WITH_OFFERS, OFFERS_PROCESSED, STOPS_IN_CELL, POIS_IN_CELL
E3: CITIES_PROCESSED, OFFERS_PER_CITY_TOTAL
```
