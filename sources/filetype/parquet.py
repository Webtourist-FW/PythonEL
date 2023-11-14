""" Definiert den Umgang mit Parquet-Quellen.

    Classes:
        ParquetSource
"""

# IMPORTS
# Generelle Importe (Python Standard Module)
import logging as log
from typing import Generator

# 3rd-Party Importe
import pandas as pd

# Eigene Module
from braxel.util.data_package import DataPackage
from braxel.sources.filetype.base import FileSource

# Set up Logger
log.getLogger(__name__).addHandler(log.NullHandler())

# FUNKTIONEN / KLASSEN


class ParquetSource(FileSource):
    """Definiert den Umgang mit Parquet-Dateien als Quelle

    Inheritance:
        FileSource: Erbt von der Datei-Quelle

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
        name_pattern: str = '',
        younger_than: str = '',
    ) -> None:
        super().__init__(
            name,
            type,
            path,
            suffix='.parquet',
            name_pattern=name_pattern,
            younger_than=younger_than,
        )

    def extract(self) -> Generator:
        """Extrahiert Daten aus Quelldateien und gibt sie als einzelne Datenpakete zurück.

        Yields:
            Generator[DataPackage]: Ein DataPackage für jede Quelldatei.
        """
        for file in self.files():
            log.info(f'{repr(self)} - Extrahiere aus Datei: {file}')
            df = pd.read_parquet(file, engine='pyarrow')
            dp = DataPackage(name=file.stem, df=df)
            yield dp
