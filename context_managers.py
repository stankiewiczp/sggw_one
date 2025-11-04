from __future__ import annotations
from pathlib import Path
from typing import Optional, Type

class Logger:
    def __enter__(self):
        print("Start sekcji logowania")
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]], exc: Optional[BaseException], tb):
        print("Koniec sekcji logowania")
        return False


class FileWriter:
    def __init__(self, path, encoding="utf-8"):
        from pathlib import Path
        self.path = Path(path)
        self.encoding = encoding
        self._fh = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.path.open("w", encoding=self.encoding)
        return self._fh

    def __exit__(self, exc_type, exc, tb):
        try:
            if self._fh:
                self._fh.close()
        except Exception as close_err:
            print(f"Błąd przy zamykaniu pliku: {close_err}")

        if exc is not None:
            print(f"Błąd podczas zapisu: {exc}")

        return False

class SafeDivision:
    def __enter__(self):
        return self

    def divide(self, a, b):
        return a / b

    def __exit__(self, exc_type: Optional[Type[BaseException]], exc: Optional[BaseException], tb):
        if exc_type is ZeroDivisionError:
            print("Nie można dzielić przez zero")
            return True
        return False


if __name__ == "__main__":
    with Logger():
        print("...wewnątrz sekcji...")

    with FileWriter("out/hello.txt") as fh:
        fh.write("Linia 1\n")
        fh.write("Linia 2\n")
    print("Zapisano do out/hello.txt")

    try:
        with FileWriter("out/will_fail.txt") as fh:
            fh.write("Zaraz rzucę wyjątek...\n")
            raise RuntimeError("Sztuczny błąd w trakcie zapisu")
    except RuntimeError as e:
        print(f"Wyjątek poprawnie nie został stłumiony: {e}")

    with SafeDivision() as sd:
        print("10 / 2 =", sd.divide(10, 2))
        print("Próba: 1 / 0 (powinien być komunikat i brak wyjątku)")
        sd.divide(1, 0) 

    try:
        with SafeDivision() as sd:
            print("Próba z typem nie-liczbowym (powinien polecieć TypeError)")
            sd.divide("a", 3) 
    except TypeError as e:
        print(f"TypeError poprawnie nie został stłumiony: {e}")
