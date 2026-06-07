Oto podsumowanie wymagań do etapu 7 (Map-Reduce), uwzględniające zarówno oficjalny harmonogram projektu, jak i doprecyzowania prowadzącego z nagrania:

**Wymagania z harmonogramu projektu:**
* **Implementacja w modelu Map-Reduce:** Przygotowanie zaplanowanych wcześniej procesów przetwarzania danych przy użyciu platformy Hadoop.
* **Minimalna liczba procesów:** Wymagane jest zaimplementowanie **co najmniej 2 procesów** przetwarzania danych.
* **Dzienniki zdarzeń (logi):** Działające procesy muszą tworzyć logi, które pokazują czas ich wykonania, status (np. sukces/porażka) oraz ewentualne błędy.
* **Wyniki i raportowanie:** Należy dostarczyć opracowany kod źródłowy oraz raport, który dokumentuje implementację i bezwzględnie zawiera pomiary czasów trwania poszczególnych etapów.

**Dodatkowe wytyczne i wymagania prowadzącego z nagrania:**
* **Zależność oceny od liczby procesów:** Zrealizowanie zaledwie 2 procesów to spełnienie minimum, które przełoży się na ocenę dostateczną (3.0). Prowadzący zaleca **zaimplementowanie wszystkich zadeklarowanych etapów przetwarzania** (od pobrania danych wejściowych do wygenerowania wyjścia), co ułatwi późniejsze zadania polegające na porównywaniu różnych środowisk.
* **Metodologia pomiarów i zbierania logów:**
    * Pomiary czasu powinny być realizowane z poziomu uruchomienia procesu ("na zewnątrz"), a nie zaszyte bezpośrednio w kodzie mapera czy reducera, aby uniknąć problemów z konsolidacją danych z różnych węzłów.
    * Należy zachowywać logi z konsoli Hadoop, ponieważ zawierają one cenne informacje (np. ile dokładnie maperów i reducerów się uruchomiło). 
    * Skonsolidowane czasy wykonania procesów powinny zostać przedstawione w raporcie **w formie tabelarycznej**.
* **Eksperymenty (warunek wyższej oceny):** Aby liczyć na ocenę wyższą niż dobra (szczególnie na bardzo dobrą), **wymagane jest przeprowadzenie 1 lub 2 eksperymentów** na jednym z procesów i udokumentowanie ich wpływu na czas przetwarzania. Prowadzący zaproponował następujące warianty eksperymentów:
    * Porównanie wpływu **liczby replik** danych w klastrze.
    * Zbadanie działania algorytmu z włączonym i wyłączonym **Kombajnerem (Combiner)**.
    * Eksperymenty ze zmienną **liczbą reducerów**.
    * Modyfikacje w konfiguracji zarządcy zadań (**scheduler z YARN**) w celu zaobserwowania, jak przydział zadań wpływa na czas wykonania.