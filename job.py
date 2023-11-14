""" Definiert Job-Klasse und sämtliche Job-spezifischen Funktionalitäten

    Classes:
        Job
"""

# IMPORTS
# Generelle Importe (Python Standard Module)
from copy import deepcopy
import logging as log

# 3rd-Party Importe

# Eigene Module
from braxel.sources.base import Source
from braxel.sources.parser import parse_source
from braxel.targets.base import Target
from braxel.targets.parser import parse_target

# Set up Logger
log.getLogger(__name__).addHandler(log.NullHandler())

# FUNKTIONEN / KLASSEN


class Job:
    """Definiert einen BRAXEL Job, welcher Daten aus einer Quelle extrahiert und in ein Ziel lädt.

        Attributes:
            name: Name des Jobs
            source: Quelle gemäß BRAXEL Quellklassen
            target: Ziel gemäß BRAXEL Zielklassen
            tags: Tags zu dem Job (mehrere Tags für einen Job möglich)

        Methods:
            run()
            from_config()
    """

    def __init__(
            self,
            name: str = 'Unnamed',
            source: Source | None = None,
            target: Target | None = None,
            tags: list | None = None) -> None:
        self.name = name
        self.source = source
        self.target = target
        self.tags = tags if tags else []
                            


    def __repr__(self) -> str:
        class_name = type(self).__name__
        return f'{class_name}(name = {self.name}, source = {repr(self.source)}, target = {repr(self.target)})'

    def __str__(self) -> str:
        return f'{self.name} ({self.source} => {self.target})'

    def run(self) -> None:
        """Führt den Job aus, indem es die Quelldaten extrahiert und ins Ziel lädt.
        """
        log.info(f'{self} wird ausgeführt...')
        try:
            datastream = self.source.extract()
        except BaseException:
            log.exception(
                f'{self} -> konnte keine Daten aus Quelle extrahieren.')
            return None
        if datastream:
            try:
                self.target.load(datastream)
            except BaseException:
                log.exception(f'{self} -> konnte keine Daten ins Ziel laden.')

    @classmethod
    def merge_config(cls, job_config: dict, connector_type: str,
                     connector_config: dict | None = None) -> dict:
        """Kombiniert die Job-Konfiguration mit der Konnektor-Konfiguration.

        Args:
            job_config (dict): Job-Konfiguration.
            connector_type (str): Art des Konnektors. "source" für Quellen. "target" für Ziele.
            connector_config (dict | None, optional): Konnektor-Konfiguration (für Quelle/Ziel). Defaults to None.

        Returns:
            dict: Gesamte Konfiguration aus Job- und Konnektor-Konfiguration
        """
        # Sicherstellen, dass nachfolgend mit Kopien gearbeitet wird
        job_config = deepcopy(job_config)
        # Konnektor-Konfiguration aus Job-Konfiguration kopieren
        connector_info = deepcopy(job_config[connector_type])
        # Falls vorhanden Konnektor-Konfigurations-Angaben hinzufügen
        if connector_config:
            connector_config = deepcopy(connector_config)
            connector_info.update(connector_config)
        return connector_info

    @classmethod
    def from_config(
            cls,
            job_config: dict,
            source_config: dict | None = None,
            target_config: dict | None = None):
        """Erstellt einen Job aus Konfigurations-Dictionaries

        Args:
            job_config (dict): Job-Konfiguration
            source_config (dict | None, optional): Quell-Konfiguration. Defaults to None.
            target_config (dict | None, optional): Ziel-Konfiguration. Defaults to None.

        Returns:
            Job: Gibt ein fertiges, nutzbares Job-Objekt zurück.
        """
        log.info(f'Erstelle Job aus Konfiguration: {job_config}')
        source_info = cls.merge_config(
            job_config=job_config,
            connector_type='source',
            connector_config=source_config
        )
        target_info = cls.merge_config(
            job_config=job_config,
            connector_type='target',
            connector_config=target_config
        )
        # Erstelle einen Job und gib ihn zurück
        source, target = None, None
        if not job_config.get('name'):
            raise ValueError(
                'Kein Job-Name angegeben. Bitte "name: JOBNAME" nachpflegen.')
        # Versuche Quelle zu erstellen
        try:
            source = parse_source(source_info)
        except BaseException:
            log.exception(
                f'Quelle konnte nicht erstellt werden. Angegebene Konfiguration: {source_info}')
        # Versuche Ziel zu erstellen
        try:
            target = parse_target(target_info)
        except BaseException:
            log.exception(
                f'Ziel konnte nicht erstellt werden. Angegebene Konfiguration: {source_info}')
        # Falls Quelle und Ziel vorhanden, erstelle den Job
        if source and target:
            return cls(
                name=job_config.get('name'),
                source=source,
                target=target,
                tags = job_config.get('tags')
            )
        else:
            raise ValueError(
                f'Job "{job_config.get("name")}" konnte nicht aus übergebener Konfiguration erstellt werden.')
