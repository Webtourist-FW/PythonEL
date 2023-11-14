""" Definiert wie das richtige Ziel aus der Konfiguration erstellt wird

    Functions:
        parse_target(target_config)
"""

# IMPORTS
# Generelle Importe (Python Standard Module)
import logging as log

# 3rd-Party Importe

# Eigene Module
from braxel.targets.base import Target
from braxel.targets.bigquery import BigQueryTarget
from braxel.targets.filetype.csv import CSVTarget
from braxel.targets.filetype.excel import ExcelTarget
from braxel.targets.filetype.parquet import ParquetTarget

# Set up Logger
log.getLogger(__name__).addHandler(log.NullHandler())

# FUNKTIONEN / KLASSEN


def parse_target(target_config: dict) -> Target:
    """Verarbeitet die Angabe des Ziel-Typen in der Konfiguration und erstellt ein passendes Ziel

    Args:
        target_config (dict): Ziel-Konfiguration

    Raises:
        KeyError: Falls kein Ziel-Typ angegeben wurde
        ValueError: Falls die Ziel-Typ Angabe nicht bekannt ist.

    Returns:
        Target: Gibt entsprechendes Ziel als nutzbares Python-Objekt zurück
    """
    # Wähle den richtigen Ziel-Typ
    match target_config.get('type'):
        case 'bigquery':
            return BigQueryTarget(**target_config)
        case 'csv':
            return CSVTarget(**target_config)
        case 'excel':
            return ExcelTarget(**target_config)
        case 'parquet':
            return ParquetTarget(**target_config)
        case other:
            if not other:
                raise KeyError(
                    f'Kein Ziel-Typ angeben. Ziel-Konfiguration muss Schlüssel "type" enthalten.')
            raise ValueError(
                f'Ziel-Typ "{other}" wurde nicht erkannt.')
