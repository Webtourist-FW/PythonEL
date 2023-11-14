""" Definiert die Basis-Quell-Klasse, welche Grundlage für alle Quell-Definitionen ist.

    Classes:
        Source
"""

# IMPORTS
# Generelle Importe (Python Standard Module)
import logging as log
from typing import Generator

# 3rd-Party Importe

# Eigene Module
from braxel.util.core import Connector

# Set up Logger
log.getLogger(__name__).addHandler(log.NullHandler())

# FUNKTIONEN / KLASSEN


class Source(Connector):
    """Basis-Quelle und damit Grundlage für alle Quell-Definitionen

        Inheritance:
            Connector: Erbt von Connector-Basis-Klasse

        Attributes:
            name: Name der Quelle
            type: Typ der Quelle

        Methods:
            extract() -> Extrahiert Daten aus der Quelle
    """

    def __init__(self, name: str, type: str) -> None:
        super().__init__(name, type)

    def extract(self) -> Generator:
        """Dient dazu die Daten aus der Quelle zu extrahieren.
            Muss in Quellen überschrieben werden.
        """
        raise NotImplementedError(
            'Extract-Methode wurde noch nicht implementiert! Sie dient dem Extrahieren der Daten.')
