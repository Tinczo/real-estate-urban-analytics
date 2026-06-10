Oto podsumowanie wymagań do etapu 11 (Cloud), uwzględniające oficjalny harmonogram projektu oraz wytyczne prowadzącego zebrane z najnowszego nagrania:

**Wymagania z harmonogramu projektu:**

- **Implementacja w chmurze:** Przeniesienie i zaimplementowanie w wybranym środowisku chmurowym (np. Databricks) procesów, które zostały zaprojektowane we wcześniejszych etapach.
- **Minimalna liczba procesów:** Wymagane jest zaimplementowanie **co najmniej 2 procesów** przetwarzania danych.
- **Dzienniki zdarzeń (logi):** Podobnie jak w poprzednich etapach, działające procesy muszą tworzyć logi dokumentujące czas wykonania, status (zakończenie z sukcesem) oraz ewentualne błędy.
- **Wyniki i raportowanie:** Należy dostarczyć opracowany kod źródłowy oraz raport, który musi zawierać udokumentowaną implementację oraz **pomiary czasów trwania poszczególnych etapów w chmurze**.

**Dodatkowe wytyczne i wymagania prowadzącego z nagrania:**

- **Porównanie środowisk:** Prowadzący bardzo zachęca do umieszczenia w raporcie wstępnego zestawienia czasów wykonania pomiędzy Sparkiem uruchomionym w chmurze, a Sparkiem wdrażanym lokalnie (on-premise). Opcjonalnie można dorzucić porównanie do Map-Reduce, chociaż to może docelowo znaleźć się w ostatnim, 12. etapie podsumowującym.
- **Eksperymenty wewnątrz Sparka (warunek wyższej oceny):** Sama implementacja kodu to za mało. Aby projekt był kompletny, prowadzący wymaga przeprowadzenia eksperymentów badawczych w chmurze. Zaproponował m.in.:
  - Porównanie wydajności kodu napisanego natywnie na **DataFrame'ach (PySpark)** z kodem wykorzystującym moduł **Spark SQL**.
  - Eksperymentowanie z różnymi **mechanizmami złączeń** (Joinów).
- **Analiza planów i grafów wykonania:** Narzędzia chmurowe dają łatwy wgląd w to, co dzieje się pod spodem. Prowadzący zaleca, by do raportu pobrać i przeanalizować:
  - **Plany wykonania zapytań SQL** ze Spark SQL.
  - **Grafy wykonania Sparka (DAG - Directed Acyclic Graph)**, które obrazują przepływ zadań.
