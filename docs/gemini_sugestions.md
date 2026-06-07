## Najistotniejsze pliki dla Etapu 7

W kontekście implementacji MapReduce, Twoim "bibliami" są:

1.  **Big_Data_Report_4.pdf (Kluczowy):** Zawiera logiczny przepis na dane. Definiuje on transformacje atrybutów dla etapów E1.5 i E2, które musisz teraz przełożyć na Javę. Znajdziesz tam precyzyjne wzory na wskaźniki (Huff, modele grawitacyjne) oraz informację o kluczu złączenia (`GEO_ID`).
2.  **Big_Data_Report_5.pdf (Techniczny):** Zawiera parametry Twojego środowiska. Musisz wiedzieć, że pracujesz na Hadoopie 3.3.0/3.3.5 z trzema węzłami roboczymi (`slave1-3`), co wpłynie na konfigurację Twojego Joba w Javie. Pokazuje też, jak monitorować postęp w interfejsie YARN.

# Wytyczne dla implementacji Etapu 7: Hadoop MapReduce (Java)

Działamy w ramach projektu "Przetwarzanie dużych zbiorów danych". Celem jest implementacja procesów przetwarzania w natywnym modelu MapReduce na platformie Hadoop (v3.3.x).

## Kontekst techniczny
- **Środowisko:** Klaster Dockerowy (1 Master, 3 Slaves), HDFS z replikacją 3.
- **Formaty danych:** Wejściowe dane to głównie Parquet (T1.1, T1.2, T1.3) oraz surowe pliki tekstowe GTFS.
- **Cel:** Implementacja co najmniej dwóch procesów z Raportu 4 (Sugerowane: E1.5 oraz E2).

## Priorytety dla Agenta LLM podczas generowania kodu:

### 1. Implementacja E1.5 (Pre-agregacja GTFS)
- **Mapper:** Filtrowanie rekordów `arrival_time` w oknie 07:00-09:00.
- **Optymalizacja:** Użyj klasy **Combiner**, aby zredukować ruch sieciowy (Shuffle) poprzez wstępne sumowanie kursów (`K_k`) na poziomie Mappera.
- **Output:** Klucz: `stop_id`, Wartość: `Sum(odjazdy)`.

### 2. Implementacja E2 (Reduce Side Join po GEO_ID)
- **Architektura:** Użyj `MultipleInputs`, aby obsłużyć różne źródła danych (T1.2 OSM, T1.3 Oferty, T1.5 Przystanki) w jednym kroku.
- **Klucz:** `GEO_ID` (Geohash 6). Zwróć uwagę na obsługę 8 sąsiednich komórek (Spatial Buffering) zdefiniowaną w projekcie.
- **Logika Reducera:** Implementacja modelu grawitacyjnego (Huffa) dla POI oraz obliczanie wskaźnika przepustowości komunikacyjnej ($K_k/d_k$).

### 3. Wymagania niefunkcjonalne (zgodnie z instrukcją projektu)
- **Custom Writables:** Zaprojektuj klasy implementujące `Writable` dla złożonych obiektów (np. atrybuty nieruchomości + wskaźniki)[cite: 1].
- **Distributed Cache:** Dla etapu E1.1 (jeśli implementowany) użyj mechanizmu `job.addCacheFile()` do Map-side Joina.
- **Logging & Monitoring:** 
    - Implementacja liczników (**Counters**) do zliczania poprawnych/błędnych rekordów.
    - Logowanie czasu trwania procesu (start/end) do standardowego wyjścia dziennika.

## Przykładowy szkielet Drivera (Maven):
Zależności: `hadoop-client` (3.3.x), `parquet-hadoop` (jeśli czytamy Parquet).