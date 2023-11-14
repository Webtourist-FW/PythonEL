""" Definiert den Umgang mit SQL-basierten Quellen

    Classes:
        SQLSource
"""

# IMPORTS
# Generelle Importe (Python Standard Module)
import logging as log
import warnings

# 3rd-Party Importe
import pandas as pd
import sqlalchemy as sa

# Eigene Module
from braxel.sources.base import Source
from braxel.util.core import sql_test_string

# Set up Logger
log.getLogger(__name__).addHandler(log.NullHandler())

# FUNKTIONEN / KLASSEN


class SQLSource(Source):
    """Definiert den Umgang mit SQL-basierten Quellen

    Inheritance:
        Source: Erbt von der Basis-Quelle

    Attributes:
        name: Name der Quelle
        type: Typ der Quelle
        host: Adresse der Datenbank
        credentials: Die Authentifizierungs-Informationen
        protocol: Das Protokoll, das für die Datenbank-Verbindung genutzt werden soll
        schema: Schema das in der Datenbank genutzt werden soll
        table: Die Tabelle die ausgelesen werden soll
        columns: Die Spalten, die aus der Tabelle extrahiert werden sollen
        where: Die Where-Clause, die angewendet werden soll
        limit: Die maximale Anzahl an Datensätzen, die zurückgegeben werden soll
    """

    def __init__(
            self,
            name: str,
            type: str,
            host: str,
            schema: str,
            table: str,
            columns: list | None = None,
            where: str = '',
            limit: int = 0,
            credentials: dict | None = None,
            protocol: str = '') -> None:
        super().__init__(name, type)
        self.host = host
        self.credentials = credentials
        self.schema = schema
        self.table = table
        self.columns = columns
        self.where = where
        self.limit = limit
        self.protocol = protocol

    def connection_string(self) -> str:
        return f'{self.protocol}://{self.credentials["user"]}:{self.credentials["password"]}@{self.host}'

    def sql(self) -> str:
        columns_string = ', '.join(self.columns) if self.columns else '*'
        sql_query = f'SELECT {columns_string} FROM {self.schema}.{self.table}'
        if self.where:
            sql_query = f'{sql_query} WHERE {self.where}'
        if self.limit:
            sql_query = f'{sql_query} LIMIT {self.limit}'
        return sql_query

    def check(self) -> bool:
        """Prüft Verbindung zur Datenbank

        Returns:
            bool: True, wenn Datenbank-Verbindung aufgebaut werden konnte.
        """
        test_sql = sql_test_string(self.type)
        log.debug(f'{repr(self)} - Setting up connection...')
        engine = sa.create_engine(self.connection_string())
        con = engine.raw_connection()
        log.debug(f'{repr(self)} - Checking connection with {test_sql}')
        # Catch Pandas Warnings
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            df = pd.read_sql(
                test_sql,
                con
            )
        return df.shape[0] > 0
