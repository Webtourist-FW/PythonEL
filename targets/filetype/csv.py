""" Definiert den Umgang mit CSV-Datei-Zielen.

    Classes:
        CSVTarget
"""

# IMPORTS
# Generelle Importe (Python Standard Module)
import logging as log
from typing import Generator

# 3rd-Party Importe
import pandas as pd

# Eigene Module
from braxel.targets.filetype.base import FileTarget

# Set up Logger
log.getLogger(__name__).addHandler(log.NullHandler())

# FUNKTIONEN / KLASSEN


class CSVTarget(FileTarget):
    """Definiert den Umgang mit CSV-Zielen

    Inheritance:
        Source: Erbt vom Datei-Ziel

    Attributes:
        name: Name des Ziels
        type: Typ des Ziels
        path: Pfad zum Verzeichnis
        filename: Gewünschter Name der Zieldatei (Default: Name der Quelle)
        separator: Trennzeichen, das genutzt werden soll
        decimal: Dezimal-Trennzeichen, das genutzt werden soll
        with_index: Ob der Index der Datensätze mit ins Ziel geladen werden soll
    """

    def __init__(
            self,
            name: str,
            type: str,
            path: str,
            filename: str = '',
            separator: str = ',',
            decimal: str = '.',
            with_index: bool = False) -> None:
        super().__init__(name, type, path, filename=filename, suffix='.csv')
        self.separator = separator
        self.decimal = decimal
        self.with_index = with_index

    def load(self, datastream: Generator) -> None:
        for dp in datastream:
            filename = self.filename if self.filename else dp.name
            target_file = self.path / filename
            target_file = target_file.with_suffix(self.suffix)
            log.info(f'{repr(self)} - Lade Daten in Datei: {target_file}')
            df = dp.df
            if target_file.exists():
                df_old = pd.read_csv(target_file)
                df = pd.concat([df_old, df])
            df.to_csv(
                target_file,
                index=self.with_index,
                sep=self.separator,
                decimal=self.decimal
            )
