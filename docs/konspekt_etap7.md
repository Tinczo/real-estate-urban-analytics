# Konspekt prezentacji – Etap 7

**Tekst do czytania na głos, strona po stronie raportu**

---

## Strona 1 – Strona tytułowa i spis treści

„Dobrze.. No to dla przypomnienia W etapie 6 napisalismy pipeline akwizycji danych z naszyuch trzech źródeł – rozkładów jazdy przez FTP, danych OpenStreetMap w oraz ofert deweloperskich z API dane.gov.pl – i załadowaliśmy je do HDFS z replikacją 3.
Teraz w etapie 7 robimy przetwarzanie tych danych. Zaimplementowaliśmy sześć jobów MapReduce,
zbadaliśmy wpływ combinera i liczby reducerów na ten proces.

---

## Strona 2 – Sekcja 1: Przegląd etapów

„Cel projektu to zbadanie czy dostępność komunikacji miejskiej i infrastruktury POI
wpływa na ceny nieruchomości w obszarze Warszawy.

Pipeline ma sześć kroków: I... E1.1 przetwarza dane GTFS – łączymy stop_times z stops
techniką Map-Side Join, bo dane przystankowe są małe i mieszczą się w pamięci każdego mappera.
E1.5 to pre-agregacja – liczymy ile odjazdów ma każdy przystanek w godzinach szczytu.

E1.2 to najdłuższy etap, bo mamy 270 milionów rekordów Open Street map i
filtrujemy je do 110 tysięcy przez PySpark, a potem przepuszczamy przez MapReduce i dajemy im geohash.

E1.3 przetwarza oferty deweloperów – dane były geocodowane wcześniej przez lokalny
serwer Nominatim, tu mapper tylko odczytuje CSV z kolumnami lat/lon i również przypisuje klucz Geohash.

E2 to centralny punkt – łączymy wyniki trzech poprzednich etapów przez Reduce-Side Join, liczymy wskaźniki

E3 liczy korelacje Pearsona między wskaźnikami a cenami, w podziale na miasta."

---

## Strona 2–3 – Sekcja 2.1: Kod E2Driver

„Przechodząc do kodu. To mamy dla przykładu driver joba E2 – tu widać jak działa MultipleInputs.

Mamy trzy wywołania `MultipleInputs.addInputPath` – każde źródło danych ma własną klasę
mappera: JoinMapperT15 dla danych GTFS, JoinMapperT12 dla OSM, JoinMapperT13 dla nieruchomości.
Wszystkie trzy emitują wspólny typ JoinRecord z kluczem Text – tym kluczem jest właśnie Geohash.
Dzięki temu Hadoop automatycznie grupuje rekordy z różnych źródeł które dotyczą tego samego
obszaru geograficznego i dostarcza je do jednego reducera.

Wejściami są SequenceFile'e "

---

## Strona 3 – Sekcja 2.2: Kod GtfsCombiner

„Drugi fragment kodu to GtfsCombiner – używany w etapie E1.5.

Combiner działa jak mini-reducer na węźle mapującym, przed wysłaniem danych przez sieć.
Widzimy że klasa dziedziczy po `Reducer<Text, StopInfo, Text, StopInfo>` – wejście i wyjście mają ten sam typ, co jest wymaganiem dla combinera.

W pętli sumujemy `totalScore` wszystkich StopInfo dla tego samego klucza, a metadane
jak lat, lon, geoId bierzemy tylko z pierwszego rekordu. "

---

## Strona 4 – Sekcja 3: Czasy wykonania

„Tabela pokazuje czasy z trzech pełnych uruchomień potoku. Dominuje E1.2 ze średnią 217 sekund – to koszt konwersji GeoParquet przez PySpark, który przetwarza właśnie dane dla całej Polski zanim wyfiltruje sam obszar Warszawy. Pozostałe etapy są szybkie – E1.1 to 20 sekund, E3 to 21 sekund. Łącznie pipeline wykonuje się w około 418 sekund."

---

## Strona 4–5 – Sekcja 4.1: Eksperyment B – Combiner

„Eksperyment pierwszy porównuje E1.5 z combinerem i bez.

Czasy wyszły praktycznie identyczne – około 19–20 sekund w obu wariantach.
Ale kluczowa jest kolumna rozmiar shuffle: bez combinera przesyłamy przez sieć 44 MB,
z combinerem tylko 0.85 MB, gdzie jest to redukcja około 50 razy.

Czasy są podobne bo zbiór GTFS dla jednego miasta jest mały – narzut uruchamiania
combinera niweluje oszczędność. Przy danych ogólnopolskich efekt czasowy byłby wyraźny."

---

## Strona 5 – Sekcja 4.2: Eksperyment C – liczba reducerów

„Eksperyment C sprawdza czy więcej reducerów przyspiesza E1.5.

Wyniki: 1 reducer – 19.6s, 2 reducery – 19.6s, 3 reducery – 20s. Brak jakiegokolwiek zysku.
Dwa powody: po pierwsze dane GTFS są nierównomiernie rozłożone – Warszawa dominuje,
więc jeden reducer zawsze jest wąskim gardłem. Po drugie – przy tak krótkim czasie obliczeń narzut uruchamiania kontenerów YARN jest porównywalny z samymi obliczeniami."

---

## Strona ostatnia – Sekcja: Logi z wykonania zadań YARN

„Ostatnia sekcja to surowe logi z wykonania joba E2.

Na początku widzimy trzy osobne linie `Total input files to process` – 12, 1 i 179 plików.
To efekt MultipleInputs – każde z trzech źródeł jest skanowane osobno przez inny mapper.
Łącznie daje to 192 splitty i tyle samo mapperów uruchomionych na klastrze.

Patrząc na postęp – reduce zaczyna się przy około 52% map. Hadoop nie czeka na koniec
wszystkich mapperów, tylko zaczyna shuffle i reduce jak tylko ma wystarczająco danych.

W Job Counters widać że 187 z 192 mapperów to Data-local map tasks – czyli YARN
postawił mappery tam gdzie fizycznie leżą bloki HDFS. To jedna z kluczowych optymalizacji
Hadoop – przenosimy obliczenia do danych, nie dane do obliczeń.
"