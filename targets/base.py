""" Definiert die Basis-Ziel-Klasse, welche Grundlage für alle Ziel-Definitionen ist.

    Classes:
        Target
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


class Target(Connector):
    """Basis-Ziel und damit Grundlage für alle Ziel-Definitionen

        Inheritance:
            Connector: Erbt von Connector-Basis-Klasse

        Attributes:
            name: Name des Ziels
            type: Typ des Ziels

        Methods:
            load() -> Lädt Daten ins Ziel
    """

    def __init__(self, name: str, type: str) -> None:
        super().__init__(name, type)

    def load(self, datastream: Generator) -> None:
        """Dient dazu die Daten ins Ziel zu laden.
            Muss in Zielen überschrieben werden.
        """
        raise NotImplementedError(
            'Load-Methode wurde noch nicht implementiert! Sie dient dem Laden der Daten ins Ziel.')
