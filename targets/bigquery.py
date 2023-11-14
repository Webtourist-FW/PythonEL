""" Definiert den Umgang mit BigQuery

    Classes:
        BigQueryTarget
"""

# IMPORTS
# Generelle Importe (Python Standard Module)
import logging as log
from pathlib import Path
from typing import Generator

# 3rd-Party Importe
from google.oauth2 import service_account as gsa
from pandas import DataFrame
import pandas_gbq as pd

# Eigene Module
from braxel.targets.base import Target
from braxel.util.core import sql_test_string

# Set up Logger
log.getLogger(__name__).addHandler(log.NullHandler())

# FUNKTIONEN / KLASSEN


class BigQueryTarget(Target):
    """Definiert den Umgang mit BigQuery als Ziel

    Inheritance:
        Target: Erbt vom Basis-Ziel

    Attributes:
        name: Name der Quelle
        type: Typ der Quelle
        project_id: Name des BigQuery-Projekts
        credentials_file: Pfad zum BigQuery Credentials-File
        dataset: Dataset in BigQuery
        table: Name der Tabelle im Dataset. (Default: Name der Quelle)
        if_exists: Was soll passieren, wenn die Tabelle bereits existiert? (Default: 'replace')
    """

    def __init__(
            self,
            name: str,
            type: str,
            project_id: str,
            credentials_file: str,
            dataset: str,
            table: str = '',
            if_exists: str = 'replace') -> None:
        super().__init__(name, type)
        self.project_id = project_id
        self.credentials_file = Path(credentials_file)
        self.dataset = dataset
        self.table = table
        self.if_exists = if_exists

    def credentials(self):
        return gsa.Credentials.from_service_account_file(self.credentials_file)

    def _correct_dtypes(self, df: DataFrame) -> DataFrame:
        """Korrigiert Datentypen mit denen BigQuery nicht umgehen kann. z.B. Datumswerte

        Args:
            df (DataFrame): DataFrame, wie er aus der Quelle kommt

        Returns:
            DataFrame: Korrigierter DataFrame
        """
        # df = dp.df.convert_dtypes(dtype_backend='pyarrow')
        for column in df:
            match df[column].dtype:
                case 'datetime.date' | 'object' | 'O':
                    df[column] = df[column].astype(str)
        return df

    def load(self, datastream: Generator) -> None:
        for dp in datastream:
            table_name = self.table if self.table else dp.name
            target_table = f'{self.dataset}.{table_name}'
            log.info(f'{repr(self)} - BigQuery Projekt: {self.project_id}')
            log.info(f'{repr(self)} - Import-Modus: {self.if_exists}')
            log.info(
                f'{repr(self)} - Lade Daten in: {self.dataset}.{target_table}')
            # df = self._correct_dtypes(dp.df)
            # Da BigQuery nicht mit Datumswerten umgehen kann (bzw. pyarrow),
            # müssen die Datentypen vor dem Upload nochmal konvertiert werden
            df = self._correct_dtypes(dp.df)
            pd.to_gbq(
                df,
                destination_table=target_table,
                project_id=self.project_id,
                if_exists=self.if_exists,
                credentials=self.credentials(),
                progress_bar=None
            )

    def check(self) -> bool:
        """Prüft Verbindung zu BigQuery

        Returns:
            bool: True, wenn Datenbank-Verbindung aufgebaut werden konnte.
        """
        test_sql = sql_test_string(self.type)
        log.debug(f'{repr(self)} - Checking connection with {test_sql}')
        df = pd.read_gbq(
            test_sql,
            project_id=self.project_id,
            credentials=self.credentials(),
            progress_bar_type=None
        )
        return df.shape[0] > 0
