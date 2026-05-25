# Moduł Gateway danych

Ten moduł jest fragmentem większego projektu akademickiego realizowanego w czteroosobowym zespole w ramach przedmiotu "Programowanie systemów czasu rzeczywistego". 

### Opis stacji C4
Udostępniony kod odpowiada za logikę stacji C4, która pełniła funkcję agregatora danych z pozostałych stanowisk.

**Główne zadania modułu:**
- Odbiór danych telemetrycznych (pogodowych i energetycznych) ze stacji C2 i C3 za pomocą protokołu **MQTT**.
- Synchronizacja danych między wątkami odbiorczymi a wątkiem zapisu przy użyciu kolejki FIFO oraz mechanizmów `std::mutex` i `std::lock_guard`.
- Zapis zgromadzonych informacji do bazy danych **InfluxDB** poprzez REST API przy użyciu biblioteki `libcurl`.

### Wdrożenie i utrzymanie
Program pracował w środowisku **Linux na serwerze VPS**. Aby zapewnić ciągłość pracy, został skonfigurowany jako **usługa systemd** (service), która monitorowała proces i automatycznie uruchamiała go ponownie po 5 sekundach w przypadku wystąpienia błędu lub zamknięcia programu.

### Wymagania techniczne
Do kompilacji wymagane są biblioteki:
- `libmosquittopp-dev`
- `libcurl4-openssl-dev`

**Polecenie kompilacji:**
```bash
g++ gateway_C4.cpp -o gateway_C4 -lmosquittopp -lcurl -lpthread
```

**Struktura plików:**
    ```text
    .
    ├── src/
    │   └── gateway_C4.cpp
    ├── .gitignore
    └── README.md
    ```