""" Definiert die Datei-Zielen und den generellen Umgang mit Dateien.

    Classes:
        FileTarget
"""

# IMPORTS
# Generelle Importe (Python Standard Module)
import logging as log
from pathlib import Path

# 3rd-Party Importe

# Eigene Module
from braxel.targets.base import Target

# Set up Logger
log.getLogger(__name__).addHandler(log.NullHandler())

# FUNKTIONEN / KLASSEN


class FileTarget(Target):
    """Definiert die Grundlogik für alle Datei-basierten Ziele

    Inheritance:
        Source: Erbt vom Basis-Ziel

    Attributes:
        name: Name des Ziels
        type: Typ des Ziels
        path: Pfad zum Verzeichnis
        filename: Gewünschter Name der Zieldatei (Default: Name der Quelle)
        suffix: Datei-Endung
    """

    def __init__(
            self,
            name: str,
            type: str,
            path: str,
            filename: str = '',
            suffix: str = '') -> None:
        super().__init__(name, type)
        self.path = Path(path)
        self.filename = filename
        self.suffix = suffix

    def check(self) -> bool:
        """Prüft ob Verzeichnis existiert, bzw. aufrufbar ist

        Returns:
            bool: True, wenn Verzeichnis existiert
        """
        return self.path.is_dir()
