""" Definiert wie die richtige Quelle aus der Konfiguration erstellt wird

    Functions:
        parse_source(source_config)
"""

# IMPORTS
# Generelle Importe (Python Standard Module)
import logging as log

# 3rd-Party Importe

# Eigene Module
from braxel.sources.base import Source
from braxel.sources.apitype.trustedshops import TrustedShopsSource
from braxel.sources.filetype.csv import CSVSource
from braxel.sources.filetype.excel import ExcelSource
from braxel.sources.filetype.parquet import ParquetSource
from braxel.sources.sqltype.db2 import DB2Source

# Set up Logger
log.getLogger(__name__).addHandler(log.NullHandler())

# FUNKTIONEN / KLASSEN


def parse_source(source_config: dict) -> Source:
    """Verarbeitet die Angabe des Quell-Typen in der Konfiguration und erstellt eine passende Quelle

    Args:
        source_config (dict): Quell-Konfiguration

    Raises:
        KeyError: Falls kein Quell-Typ angegeben wurde
        ValueError: Falls die Quell-Typ Angabe nicht bekannt ist.

    Returns:
        Source: Gibt entsprechende Quelle als nutzbares Python-Objekt zurück
    """
    # Wähle den richtigen Quell-Typ
    match source_config.get('type'):
        case 'csv':
            return CSVSource(**source_config)
        case 'db2':
            return DB2Source(**source_config)
        case 'excel':
            return ExcelSource(**source_config)
        case 'parquet':
            return ParquetSource(**source_config)
        case 'trustedshops':
            return TrustedShopsSource(**source_config)
        case other:
            if not other:
                raise KeyError(
                    f'Kein Quell-Typ angeben. Quell-Konfiguration muss Schlüssel "type" enthalten.')
            raise ValueError(
                f'Quell-Typ "{other}" wurde nicht erkannt.')
