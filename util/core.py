""" Definiert einige grundsätzliche Utility-Funktionalitäten

    Classes:
        Connector -> Basis-Klasse für alle Quellen und Ziele

    Functions:
        load_yaml(path)
        flatten_json(nested_json, exclude, separator)
"""

# IMPORTS
# Generelle Importe (Python Standard Module)
import logging as log
from pathlib import Path

# 3rd-Party Importe
import yaml

# Eigene Module

# Set up Logger
log.getLogger(__name__).addHandler(log.NullHandler())

# FUNKTIONEN / KLASSEN


class Connector:
    """Basis-Konnektor und damit Grundlage für alle Quell- und Ziel-Definitionen

        Attributes:
            name: Name des Ziels
            type: Typ des Ziels

        Methods:
            check() -> Prüft die Verbindung. Muss in Quelle/Ziel definiert werden.
            debug(config: dict) -> Prüft, ob der Konnektor (Quelle/Ziel) aus der Konfiguration erstellt werden kann.
    """

    def __init__(self, name: str, type: str) -> None:
        self.name = name
        self.type = type

    def __repr__(self) -> str:
        class_name = type(self).__name__
        return f'{class_name}(name = {self.name})'

    def __str__(self) -> str:
        return f'{self.name} [{self.type}]'

    def check(self):
        """Dient dazu die Verbindung des Konnektors zu prüfen.
            Muss in Quelle/Ziel überschrieben werden.
        """
        raise NotImplementedError(
            'Check-Methode wurde noch nicht implementiert! Sie dient dem Überprüfen der Verbindung.')

    @classmethod
    def debug(cls, config: dict) -> bool:
        """Überprüft, ob aus der Konnektor aus der übergebenen Konfiguration erstellt werden kann.

        Args:
            config (dict): Konfiguration des Konnektors

        Returns:
            bool: "True", falls das erstellen möglich ist. Ansonsten "False".
        """
        log.debug(f'Versuche {cls.__name__} aus Konfiguration zu erstellen.')
        try:
            cls(**config)
            log.debug(
                f'{cls.__name__} kann erfolgreich aus Konfiguration erstellt werden: {config}')
            return True
        except BaseException:
            log.debug(
                f'{cls.__name__} kann nicht aus Konfiguration erstellt werden: {config}')
            return False


def sql_test_string(db: str = '') -> str:
    """Erstellt SQL Test-Statement für jede Datenbank. Basierend auf DUAL-Tabelle.
    Siehe: https://www.jooq.org/doc/latest/manual/sql-building/table-expressions/dual/

    Args:
        db (str, optional): Datenbank. Defaults to ''.

    Returns:
        str: Test-SQL-Statement nach Schema "SELECT 1"
    """
    match db:
        case 'bigquery' | 'exasol' | 'mariadb' | 'mysql' | 'oracle' | 'postgres' | 'redshift' | 'snowflake' | 'sqlite' | 'sqlserver':
            return 'SELECT 1'
        # DB2 Variante aus Doku funktioniert nicht, da DUAL nicht angelegt. Dafür aber SYSDUMMY1
        # case 'db2':
            # return 'SELECT 1 FROM SYSIBM.DUAL'
        case 'db2' | 'derby':
            return 'SELECT 1 FROM SYSIBM.SYSDUMMY1'
        case 'hana' | 'sybase':
            return 'SELECT 1 FROM SYS.DUMMY'
        case 'aurora_mysql' | 'memsql':
            return 'SELECT 1 FROM DUAL'
        case 'access':
            'SELECT 1 FROM (SELECT count(*) dual FROM MSysResources) AS dual'
        case 'hsqldb':
            'SELECT 1 FROM (VALUES(1)) AS dual(dual)'
        case 'informix':
            'SELECT 1 FROM (SELECT 1 AS dual FROM systables WHERE (tabid = 1)) AS dual'
        case 'teradata':
            'SELECT 1 FROM (SELECT 1 AS "dual") AS dual'
        case 'firebird':
            'SELECT 1 FROM RDB$DATABASE'
        case _:
            return 'SELECT 1'


def load_yaml(path: str | Path) -> dict:
    """Lädt Daten aus YAML-Datei und gibt sie als Python dictionary zurück.

    Args:
        path (str | Path): Pfad zur YAML-Datei

    Returns:
        dict: Inhalt der YAML-Datei als Python-Dictionary
    """
    path = Path(path)
    with path.open('r', encoding='utf-8') as config_file:
        config = yaml.safe_load(config_file)
    return config


def flatten_json(
        nested_json: dict,
        exclude: list | None = None,
        separator: str = '_') -> dict:
    """Flatten a list of nested dicts.

    Args:
        nested_json (dict): JSON object (dict)
        exclude (list | None, optional): List of keys to exclude. Defaults to [''].
        separator (str, optional): Separator to use, when combining keys. Defaults to '_'.

    Returns:
        dict: Flattened python dictionary, with key-value-pairs
    """
    out = dict()
    if not exclude:
        exclude = ['']

    def flatten(x: list | dict | str, name: str = '', exclude=exclude):
        if isinstance(x, dict):
            for a in x:
                if a not in exclude:
                    flatten(x[a], f'{name}{a}{separator}')
        elif isinstance(x, list):
            i = 0
            for a in x:
                flatten(a, f'{name}{i}{separator}')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(nested_json)
    return out
