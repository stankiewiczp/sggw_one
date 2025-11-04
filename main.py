from pathlib import Path
import requests
import math
import csv
from functools import wraps
from time import perf_counter
from datetime import datetime



# ĆW.1 użyć requests aby ściągnąc plik csv.
# ĆW.2 dodać dwa własne wyjątki
# Ćw.3 Transformacja plików przy pomocy generetorów. Wczytuje plik linijka po linijce, liczy SUM, AVG, zapisuje indeksy 
# brakujących wartości (list comprehension) do dwóch plików: values.csv, missing_values.csv. Pierwszy z kolumnami: nr porządkowy, SUM, AVG.
# Drugi: nr porządkowy, indeksy kolumn z myślnikami zamiast wartości

def log_timing(label: str | None = None):
    """
    Dekorator z ćw. IV
    Użycie:
    @log_timing()             # nazwa z funkcji
    @log_timing("ETL CSV")    # własna etykieta
    """
    def _decorator(fn):
        @wraps(fn)
        def _wrapped(*args, **kwargs):
            name = label or fn.__name__
            start_wall = datetime.now()
            start = perf_counter()
            print(f"[{name}] start: {start_wall.isoformat(sep=' ', timespec='seconds')}")
            try:
                return fn(*args, **kwargs)
            finally:
                end = perf_counter()
                end_wall = datetime.now()
                print(f"[{name}]  stop: {end_wall.isoformat(sep=' ', timespec='seconds')}  "
                      f"elapsed: {end - start:.6f}s")
        return _wrapped
    return _decorator


class NotFoundError(Exception):
    """Rzucane gdy serwer zwraca HTTP 404 (Not Found)."""
    code = 404
    def __init__(self, url: str, detail: str | None = None):
        self.url = url
        self.detail = detail
        super().__init__(f"Not found (404): {url}" + (f" — {detail}" if detail else ""))
    pass

class AccessDeniedError(Exception):
    """Rzucane gdy serwer zwraca HTTP 403 (Service Unavailable / denied)."""
    code = 403
    def __init__(self, url: str, detail: str | None = None):
        self.url = url
        self.detail = detail
        super().__init__(f"Service Unavailable (403): {url}" + (f" — {detail}" if detail else ""))
    pass

DEFAULT_URL = "https://oleksandr-fedoruk.com/wp-content/uploads/2025/10/sample.csv"
DEFAULT_NAME = "latest.txt"

def get_csv(filename: str = DEFAULT_NAME, url: str = DEFAULT_URL) -> Path:
    """
    Ćwiczenie 1. Funkcja zapisuje plik z domyślnego adresu, jeśli nie zostanie podany.
    Ćwiczenie 2. Rzuca:
      - NotFoundError dla 404
      - AccessDeniedError dla 403
      - requests.HTTPError dla innych błędów 4xx/5xx
    """

    dest = Path(filename)
    dest.parent.mkdir(parents=True, exist_ok=True)
    resp = requests.get(url, timeout=10.0)

    # Najpierw mapujemy konkretne kody na własne wyjątki:
    if resp.status_code == 404:
        raise NotFoundError(f"Nie znaleziono zasobu: {url}")
    if resp.status_code == 403:
        raise AccessDeniedError(f"Dostęp zabroniony lub usługa niedostępna: {url}")

    # Dla pozostałych błędów HTTP:
    resp.raise_for_status()

    dest.write_bytes(resp.content)
    print(dest)
    return dest


@log_timing("CSV ETL")
def process_csv_etl(input_path: str | Path,
                    out_values: str | Path = "values.csv",
                    out_missing: str | Path = "missing_values.csv",
                    decimal_comma: bool = False,
                    encoding: str = "utf-8") -> None:
    """
    Ćw. III — ETL
    """
    input_path = Path(input_path)
    out_values = Path(out_values)
    out_missing = Path(out_missing)
    out_values.parent.mkdir(parents=True, exist_ok=True)
    out_missing.parent.mkdir(parents=True, exist_ok=True)

    # generatory

    def read_rows():
        with input_path.open("r", encoding=encoding, newline="") as f:
            for line in f:
                yield line

    # złapmy kilkanaście linii do sniffowania delimitera
    row_iter = read_rows()
    preview = []
    for _, line in zip(range(50), row_iter):
        preview.append(line)
    sample = "".join(preview)

    # autodetekcja delimitera (przecinek/średnik/tab)
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t"])
    except csv.Error:
        class _Fallback(csv.Dialect):
            delimiter = ","
            quotechar = '"'
            escapechar = None
            doublequote = True
            skipinitialspace = False
            lineterminator = "\n"
            quoting = csv.QUOTE_MINIMAL
        dialect = _Fallback()

    def chain_preview():
        for l in preview:
            yield l
        for l in row_iter:
            yield l

    def parse_rows():
        reader = csv.reader(chain_preview(), dialect=dialect)
        for fields in reader:
            if fields:
                yield fields

    def to_float(s: str) -> float:
        s = s.strip()
        if decimal_comma:
            s = s.replace(",", ".")  # tylko dziesiętny, bo delimiter mamy już rozdzielony
        return float(s)

    def transform():
        """
        Yielduje: (nr, suma, avg, missing_idx_list)
        gdzie missing_idx liczone od 1 względem kolumn danych (czyli druga kolumna w pliku to indeks 1).
        """
        for fields in parse_rows():
            nr = fields[0]
            values = fields[1:]

            missing_idx = [i for i, v in enumerate(values, start=1) if v.strip() == "-"]

            nums = []
            for v in values:
                v = v.strip()
                if v == "-" or v == "":
                    continue
                try:
                    nums.append(to_float(v))
                except ValueError:
                    # śmieci jako brak
                    continue

            total = sum(nums)
            avg = (total / len(nums)) if nums else float("nan")
            yield nr, total, avg, missing_idx

    # --- Zapis dwóch plików ---
    with out_values.open("w", encoding=encoding, newline="") as fv, \
         out_missing.open("w", encoding=encoding, newline="") as fm:
        w_values = csv.writer(fv)
        w_missing = csv.writer(fm)

        w_values.writerow(["numer_porządkowy", "SUM", "AVG"])
        w_missing.writerow(["numer_porządkowy", "indeksy_braków"])

        for nr, total, avg, missing_idx in transform():
            w_values.writerow([nr, f"{total}", ("" if math.isnan(avg) else f"{avg}")])
            w_missing.writerow([nr, " ".join(map(str, missing_idx))])


if __name__ == "__main__":
    try:
        # Domyślny URL
        saved_path = get_csv("downloads/sample.csv", "https://oleksandr-fedoruk.com/wp-content/uploads/2025/10/sample.csv")
        print(f"Ćwiczenie Nr 1, zapisano do: {saved_path}")
    
    except NotFoundError as e:
        print(f"Błąd 404: {e}")
    except AccessDeniedError as e:
        print(f"Błąd 503: {e}")
    except requests.HTTPError as e:
        print(f"Błąd HTTP: {e.response.status_code} {e}")
    except requests.RequestException as e:
        print(f"Błąd sieci: {e}")

    
    process_csv_etl(saved_path, "values.csv", "missing_values.csv", decimal_comma=False)
    print("Gotowe: values.csv oraz missing_values.csv")

       
