""" Definiert die Datei-Quellen und den generellen Umgang mit Dateien.

    Classes:
        FileSource
"""

# IMPORTS
# Generelle Importe (Python Standard Module)
import logging as log
from pathlib import Path

# 3rd-Party Importe

# Eigene Module
from braxel.sources.base import Source
from braxel.util.time_handling import is_younger_than, get_datetime_from_timedelta

# Set up Logger
log.getLogger(__name__).addHandler(log.NullHandler())


# FUNKTIONEN / KLASSEN
class FileSource(Source):
    """Definiert die Grundlogik für alle Datei-basierten Quellen

    Inheritance:
        Source: Erbt von der Basis-Quelle

    Attributes:
        name: Name der Quelle
        type: Typ der Quelle
        path: Pfad zum Verzeichnis
        name_pattern: Namensschema nach dem im Verzeichnis gefilter werden soll
        younger_than: Maximal gültiges Alter (relativ zum heutigen Tag) der Dateien
        suffix: Datei-Endung
    """

    def __init__(
        self,
        name: str,
        type: str,
        path: str,
        suffix: str = '',
        name_pattern: str = '*',
        younger_than: str = '',
    ) -> None:
        super().__init__(name, type)
        self.path = Path(path)
        self.name_pattern = name_pattern
        self.younger_than = younger_than
        self.max_age = None
        self.suffix = suffix
        # Maximales Alter (Datum) automatisch setzen, falls younger_than
        # übergeben wurde
        self.calculate_max_age()

    def calculate_max_age(self):
        """Berechnet und setzt maximales Alter der Dateien (Datum) basierend auf self.younger_than."""
        self.max_age = get_datetime_from_timedelta(self.younger_than)
        return self.max_age

    def is_valid_file(self, path: Path) -> bool:
        """Prüft ob ein Dateipfad eine gültige Quelle ist.

        Args:
            path (Path): Datei-Pfad der zu prüfen ist.

        Returns:
            bool: True, wenn Dateipfad gültig.
        """
        if not path.parent == self.path:
            return False
        if not path.is_file():
            return False
        if self.suffix and not path.suffix.lower() == self.suffix:
            return False
        if self.name_pattern and not path.match(self.name_pattern):
            return False
        if self.younger_than and not is_younger_than(path, self.max_age):
            return False
        return True

    def check(self) -> bool:
        """Prüft ob Verzeichnis existiert, bzw. aufrufbar ist

        Returns:
            bool: True, wenn Verzeichnis existiert
        """
        return self.path.is_dir()

    def files(self) -> list[Path]:
        """Gibt alle gültigen Dateien zurück.

        Yields:
            list[Path]: Ein Pfad-Objekt für jede gültige Datei.
        """
        file_list = [path for path in self.path.iterdir()
                     if self.is_valid_file(path)]
        if not file_list:
            log.warn(f"{self} - Keine gültigen Dateien gefunden.")
        return file_list
