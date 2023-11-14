""" Definiert den Umgang mit Excel-Datei-Zielen.

    Classes:
        ExcelTarget
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


class ExcelTarget(FileTarget):
    """Definiert den Umgang mit Excel-Zielen

    Inheritance:
        Source: Erbt vom Datei-Ziel

    Attributes:
        name: Name des Ziels
        type: Typ des Ziels
        path: Pfad zum Verzeichnis
        filename: Gewünschter Name der Zieldatei (Default: Name der Quelle)
        sheet_name: Name des Tabellen-Reiters in den die Daten in Excel geladen werden sollen
        with_index: Ob der Index der Datensätze mit ins Ziel geladen werden soll
    """

    def __init__(
            self,
            name: str,
            type: str,
            path: str,
            filename: str = '',
            with_index: bool = False,
            sheet_name: str = 'Tabelle1') -> None:
        super().__init__(name, type, path, filename=filename, suffix='.xlsx')
        self.with_index = with_index
        self.sheet_name = sheet_name

    def load(self, datastream: Generator) -> None:
        for dp in datastream:
            filename = self.filename if self.filename else dp.name
            target_file = self.path / filename
            target_file = target_file.with_suffix(self.suffix)
            log.info(f'{repr(self)} - Lade Daten in Datei: {target_file}')
            df = dp.df
            if target_file.exists():
                df_old = pd.read_excel(target_file, sheet_name=self.sheet_name)
                df = pd.concat([df_old, df])
            df.to_excel(
                target_file,
                index=self.with_index,
                sheet_name=self.sheet_name)
