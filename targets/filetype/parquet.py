""" Definiert die Parquet-Datei-Zielen.

    Classes:
        ParquetTarget
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


class ParquetTarget(FileTarget):
    """Definiert den Umgang mit Parquet-Zielen

    Inheritance:
        Source: Erbt vom Datei-Ziel

    Attributes:
        name: Name des Ziels
        type: Typ des Ziels
        path: Pfad zum Verzeichnis
        filename: Gewünschter Name der Zieldatei (Default: Name der Quelle)
    """

    def __init__(
            self,
            name: str,
            type: str,
            path: str,
            filename: str = '') -> None:
        super().__init__(name, type, path, filename=filename, suffix='.parquet')

    def load(self, datastream: Generator) -> None:
        for dp in datastream:
            filename = self.filename if self.filename else dp.name
            target_file = self.path / filename
            target_file = target_file.with_suffix(self.suffix)
            log.info(f'{repr(self)} - Lade Daten in Datei: {target_file}')
            df = dp.df
            if target_file.exists():
                df_old = pd.read_parquet(target_file, engine='pyarrow')
                df = pd.concat([df_old, df])
            df.to_parquet(target_file, engine='pyarrow', compression='snappy')
