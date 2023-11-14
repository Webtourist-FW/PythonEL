""" Definiert das Austauschformat zwischen Quellen und Zielen

    Classes:
        DataPackage
"""

# IMPORTS
# Generelle Importe (Python Standard Module)
import logging as log

# 3rd-Party Importe
import pandas as pd

# Eigene Module

# Set up Logger
log.getLogger(__name__).addHandler(log.NullHandler())

# FUNKTIONEN / KLASSEN


class DataPackage:
    """Definiert das Austausch-Format zwischen Quelle und Ziel

        Attributes:
            name: Name des Daten-Pakets
            df: Daten (Inhalt) des Daten-Pakets als Pandas DataFrame
    """

    def __init__(self, name: str, df: pd.DataFrame = None) -> None:
        self.name = name
        self.df = df

    def __repr__(self) -> str:
        class_name = type(self).__name__
        return f'{class_name}(name = {self.name})'

    def __str__(self) -> str:
        return f'{self.name}'
