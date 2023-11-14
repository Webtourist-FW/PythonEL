""" Definiert den Umgang mit DB2-Quellen

    Classes:
        DB2Source
"""

# IMPORTS
# Generelle Importe (Python Standard Module)
import logging as log
import re
from typing import Generator

# 3rd-Party Importe
import pandas as pd
import sqlalchemy as sa

# Eigene Module
from braxel.util.data_package import DataPackage
from braxel.sources.sqltype.base import SQLSource

# Set up Logger
log.getLogger(__name__).addHandler(log.NullHandler())

# FUNKTIONEN / KLASSEN


class DB2Source(SQLSource):
    """Definiert den Umgang mit DB2-Quellen

    Inheritance:
        SQLSource: Erbt von der SQL-Quelle

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
            credentials: dict | None = None) -> None:
        super().__init__(
            name,
            type,
            host,
            schema,
            table,
            columns,
            where,
            limit,
            credentials,
            protocol='ibmi')

    def _preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        """Verarbeitet die ausgelesenen Daten und korrigiert eventuelle Fehler (z.B. falsche Datentypen-Erkennung).

        Args:
            df (pd.DataFrame): Der ausgelesene eventuell fehlerhafte DataFrame

        Returns:
            pd.DataFrame: Ein verarbeiteter, korrigierter DataFrame
        """
        def replace_empty_string(value):
            only_spaces = re.compile(r'^ +$')  # z.B. '         '
            only_dashes = re.compile(r'^-+$')  # z.B. '-----'
            if not pd.isna(value) and (
                    only_spaces.match(value) or only_dashes.match(value)):
                return ''
            return value
        # Konvertiere Die Datentypen nochmal richtig (z.B: für Integer mit NaN)
        df = df.convert_dtypes()
        # Mache je Spalte Typ-Spezifische Anpassungen (z.B:bei Strings)
        for column in df:
            if df[column].dtype == 'string[python]':
                df[column] = df[column].apply(replace_empty_string)
        return df

    def extract(self) -> Generator:
        log.info(f'{repr(self)} - Extrahiere Daten aus: {self.table}')
        engine = sa.create_engine(self.connection_string())
        con = engine.raw_connection()
        log.info(
            f'{repr(self)} - Datenbank-Verbindung aufgebaut '
            f'(Host: "{self.host}", Protokoll: "{self.protocol}")')
        log.info(f'{repr(self)} - Nutze SQL Query: "{self.sql()}"')
        df_raw = pd.read_sql(self.sql(), con)
        df = self._preprocess(df_raw)
        dp = DataPackage(name=self.table, df=df)
        yield dp
