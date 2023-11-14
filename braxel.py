""" Grundlogik. Definiert Methoden um Konfiguration zu laden und zu verarbeiten, Jobs zu erstellen und auszuführen.

    Classes:
        PyEL

"""

# IMPORTS
# Generelle Importe (Python Standard Module)
from collections import namedtuple
import logging as log
from multiprocessing import Process
from typing import Generator

# 3rd-Party Importe
from colorama import just_fix_windows_console, Fore

# Eigene Module
from braxel.environment import BraxelEnvironment
from braxel.job import Job
from braxel.util.core import load_yaml

# Set up Logger
log.getLogger(__name__).addHandler(log.NullHandler())

# FUNKTIONEN / KLASSEN


class Braxel:
    """Braxel bietet die Möglichkeit Jobs per Konfiguration zu erstellen und auszuführen.

        Methods:
            jobs()
            filter_jobs()
            run()
    """

    def __init__(self,
                 jobs: list | None = None,
                 sources: dict | None = None,
                 targets: dict | None = None) -> None:
        """Initialisiert Braxel-Objekt.

        Args:
            jobs (list | None, optional): Liste mit Job-Konfigurationen. Defaults to None.
            sources (dict | None, optional): Quell-Konfigurationen. Defaults to None.
            targets (dict | None, optional): Ziel-Konfigurationen. Defaults to None.
           
        """
        self._env = BraxelEnvironment()
        self._jobs_config = self._load_jobs_config(jobs)
        self._sources_config = self._load_sources_config(sources)
        self._targets_config = self._load_targets_config(targets)

    def _load_jobs_config(self, job_config: list | None = None) -> list:
        """Lädt Job-Konfiguration per Umgebungsvariable, falls nicht direkt übergeben

        Args:
            job_config (list | None, optional): Liste mit Job-Konfigurationen. Defaults to None.

        Raises:
            ValueError: Falls keine Job-Konfiguration gefunden.

        Returns:
            list: Liste mit Job-Konfigurationen
        """
        if job_config:
            return job_config
        if self._env.jobpath():
            return load_yaml(self._env.jobpath())
        raise ValueError(
            'Es muss eine Job-Liste übergeben werden oder per Umgebungvariable aufrufbar sein.')

    def _load_sources_config(self,
                             source_config: list | None = None) -> dict | None:
        """Lädt Quell-Konfigurationen per Umgebungsvariable, falls nicht direkt übergeben.

        Args:
            source_config (list | None, optional): Quell-Konfigurationen. Defaults to None.

        Returns:
            dict | None: Quell-Konfigurationen
        """
        if source_config:
            return source_config
        if self._env.sourcespath():
            return load_yaml(self._env.sourcespath())
        log.info(
            f'Keine Quell-Konfiguration übergeben oder per Umgebungsvariable gefunden. '
            f'Verwende ausschließlich Angaben aus Job-Konfiguration.')

    def _load_targets_config(self,
                             target_config: list | None = None) -> dict | None:
        """Lädt Ziel-Konfigurationen per Umgebungsvariable, falls nicht direkt übergeben.

        Args:
            source_config (list | None, optional): Ziel-Konfigurationen. Defaults to None.

        Returns:
            dict | None: Ziel-Konfigurationen
        """
        if target_config:
            return target_config
        if self._env.targetspath():
            return load_yaml(self._env.targetspath())
        log.info(
            f'Keine Ziel-Konfiguration übergeben oder per Umgebungsvariable gefunden. '
            f'Verwende ausschließlich Angaben aus Job-Konfiguration.')

    def _get_source_config(self, name: str) -> dict | None:
        """Sucht bestimmte Quell-Konfiguration per Name.

        Args:
            name (str): Name der Quelle

        Returns:
            dict | None: Quell-Konfiguration. GIbt None zurück falls keine valide Quell-Konfiguration gefunden.
        """
        if not self._sources_config:
            return None
        source_config = self._sources_config.get(name)
        if not source_config:
            log.info(
                f'Quelle "{name}" nicht in Quell-Konfigurations-Datei. '
                f'Es werden ausschließlich Angaben aus Job-Konfiguration genutzt.')
        return source_config

    def _get_target_config(self, name: str) -> dict | None:
        """Sucht bestimmte Ziel-Konfiguration per Name.

        Args:
            name (str): Name des Ziels

        Returns:
            dict | None: Ziel-Konfiguration. GIbt None zurück falls keine valide Ziel-Konfiguration gefunden.
        """
        if not self._targets_config:
            return None
        target_config = self._targets_config.get(name)
        if not target_config:
            log.info(
                f'Ziel "{name}" nicht in Ziel-Konfigurations-Datei. '
                f'Es werden ausschließlich Angaben aus Job-Konfiguration genutzt.')
        return target_config

    def jobs(self) -> Generator:
        """Erstellt Jobs und gibt sie einzeln zurück.

        Yields:
            Generator: Liefert einen Job pro Iteration
        """
        for job_config in self._jobs_config:
            source_config = self._get_source_config(
                job_config['source']['name'])
            target_config = self._get_target_config(
                job_config['target']['name'])
            # Job erstellen
            try:
                job = Job.from_config(
                    job_config=job_config,
                    source_config=source_config,
                    target_config=target_config
                )
                log.info(f'Job "{job.name}" mit Tag "{job.tags}" erfolgreich erstellt.')
                yield job
            except BaseException:
                log.exception(
                    f'Job "{job_config.get("name")}" konnte nicht erstellt werden.')

    def filter_jobs(self, name: str = '', tag: str = '') -> list[Job]:
        """Filtert Jobs.

        Args:
            name (str, optional): Job-Name nach dem gefiltert werden soll. Defaults to ''.
            tag (str, optional): Tag-Name nach dem gefiltert werden soll, alle jobs zu einem Tag werden ausgeführt. Defaults to ''.

        Returns:
            list[Job]: Gefilterte Job-Liste. Falls keine Filter mitgegeben, werden (Job oder Tag) alle Jobs zurückgegeben.
        """
        filtered_jobs = list(self.jobs())
        
        if name or tag:
            filtered_jobs_tags = [job for job in filtered_jobs if tag in job.tags]
            filtered_jobs = [job for job in filtered_jobs if name == job.name]
            filtered_jobs = list(set(filtered_jobs + filtered_jobs_tags))
            num_jobs = len(filtered_jobs)
            log.info(
                f'{num_jobs} Job{"s" if num_jobs != 1 else ""} mit Name "{name}" ausgewählt.')
        return filtered_jobs

    def run(self, job_name: str = '', tag_name: str = '', run_parallel: bool = False) -> None:
        """Führt Jobs aus.

        Args:
            job_name (str, optional): Nur Jobs mit diesem Namen werden ausgeführt. Defaults to ''.
            tag_name (str, optional): Nur Jobs mit diesem Tag werden ausgeführt. Defaults to ''.
            run_parallel (bool, optional): Startet alle Jobs parallel als eigene Prozesse. Defaults to False.
        """
        # Warne vor fehlerhaften Log-Verhalten bei Multi-Processing.
        # (!) Warnung entfernen, wenn Multi-Processing Logging umgesetzt.
        if run_parallel:
            log.warn(
                f'BRAXEL läuft mit Multi Processing. '
                f'Jobs werden parallel ausgeführt. '
                f'Logging ist funktioniert nur teilweise.'
            )
        job_list = self.filter_jobs(name=job_name, tag = tag_name)
        if not job_list:
            log.info('Keine validen Jobs gefunden. Braxel Run wird abgebrochen.')
        for job in job_list:
            if run_parallel:
                process = Process(target=job.run)
                process.start()
            else:
                job.run()

    @classmethod
    def debug(cls,
              jobs: list | None = None,
              sources: dict | None = None,
              targets: dict | None = None,
              job_name: str = '',
              tag_name: str = '') -> dict:
        """Prüft jede einzelne Job-Konfiguration. Kann der Job erstellt werden? Wenn ja, kann
        eine Verbindung zur Quelle und zum Ziel aufgebaut werden?

        Args:
            jobs (list | None, optional): Job-Konfiguration. Wenn nicht angegeben wird die Umgebungsvariable genutzt. Defaults to None.
            sources (dict | None, optional): Quell-Konfiguration. Wenn nicht angegeben wird die Umgebungsvariable genutzt. Defaults to None.
            targets (dict | None, optional): Ziel-Konfiguration. Wenn nicht angegeben wird die Umgebungsvariable genutzt. Defaults to None.
            job_name (str, optional): Name des Jobs nach dem gefiltert werden soll. Defaults to ''.
            tag_name (str, optional): Name des Tags nach dem die Jobs gefiltert werden soll. Defaults to ''.

        Returns:
            dict: Ergebnis als Dictionary nach Schema {
                JOBNAME: {
                    creatable: True/False,
                    source_connectable: True/False,
                    target_connectable: True/False
                }
            }
        """
        log.debug(f'========  Starte Debug-Lauf für BRAXEL. ========')
        # Initialisiere Shell mit ANSI Farben in MS Windows
        just_fix_windows_console()
        print(f'\n{Fore.RESET}Beginne Debug-Lauf...\n')
        # Initialisiere Ergebnis-Dict
        debug_result = {}
        # Versuche BRAXEL zu initialisieren
        braxel = cls(jobs, sources, targets)
        jobs_created = list(braxel.jobs())
        jobs_requested = [
            job for job in braxel._load_jobs_config(jobs) if job.get('name')]
        # Filtere Jobs über den Jobnamen oder den Tag falls nicht alle Jobs getestet werden sollen
        if job_name or tag_name:
            print(f'{Fore.RESET}Filtere Jobs auf Name: {job_name}\n')
            print(f'{Fore.RESET}Filtere Jobs auf Tag: {tag_name}\n')
            # Filtern der erstellten Jobs
            jobs_created_tags = [job for job in jobs_created if tag_name in job.tags]
            jobs_created = [job for job in jobs_created if job.name == job_name]
            jobs_created = list(set(jobs_created + jobs_created_tags))
            # Filtern der konfigurierten Jobs   
            jobs_requested_tags = [job for job in jobs_requested if tag_name in job.get('tags', [])]
            jobs_requested = [job for job in jobs_requested if job.get('name') == job_name]
            jobs_requested += [job for job in jobs_requested_tags if job not in jobs_requested]
        # Filtere nach Jobs ohne Namen
        jobs_without_name = [
            job for job in jobs_requested if not job.get('name')]
        # Gebe die Anzahl an erfolgreich erstellen Jobs aus
        num_jobs_created = len(jobs_created)
        num_jobs_requested = len(jobs_requested)
        num_jobs_without_name = len(jobs_without_name)
        if num_jobs_without_name:
            print(
                f'{Fore.RED}Anzahl Jobs ohne Namen: {num_jobs_without_name}. Bitte Name nachpflegen.\n')
        if num_jobs_requested == 0:
            print(f'{Fore.RED}Keine Jobs in Konfiguration gefunden.\n')
        elif num_jobs_created == num_jobs_requested:
            print(
                f'{Fore.GREEN}Alle Jobs ({num_jobs_created}/{num_jobs_requested}) konnten erfolgreich erstellt werden.\n')
        elif num_jobs_created > 0 and num_jobs_created < num_jobs_requested:
            print(
                f'{Fore.YELLOW}{num_jobs_created}/{num_jobs_requested} Jobs konnten erstellt werden.\n')
        elif num_jobs_created == 0:
            print(
                f'{Fore.RED}Keiner der Jobs aus der Konfiguration konnte erstellt werden.\n')
        # Gebe die Namen der nicht erstellten Jobs aus
        if num_jobs_created < num_jobs_requested:
            job_names_created = {job.name for job in jobs_created}
            job_names_requested = {job.get('name') for job in jobs_requested}
            jobs_not_created = job_names_requested - job_names_created
            jobs_not_created = sorted(jobs_not_created)
            print(f'{Fore.RED}Die nachfolgenden Jobs konnten nicht erstellt werden:')
            print(f'{Fore.RED}{jobs_not_created}\n')
            for name in jobs_not_created:
                debug_result[name] = {
                    'creatable': False,
                    'source_connectable': False,
                    'target_connectable': False}
        # Validiere die Quell- und Ziel-Konfiguration jedes erstellten Jobs
        if num_jobs_created > 0:
            source_connectable, target_connectable = False, False
            print(f'{Fore.RESET}Erstellte Jobs:\n')
            for job in jobs_created:
                print(f'{Fore.RESET}{job}')
                try:
                    source_connectable = job.source.check()
                    if source_connectable:
                        print(
                            f'\t{Fore.GREEN}Quell-Verbindung konnte aufgebaut werden.')
                    else:
                        print(
                            f'\t{Fore.RED}Quell-Verbindung konnte nicht aufgebaut werden.')
                except NotImplementedError:
                    print(
                        f'\t{Fore.YELLOW}Quelle unterstützt check() noch nicht.')
                except BaseException:
                    log.exception(
                        'Quell-Verbindung konnte nicht getestet werden.')
                    print(
                        f'\t{Fore.RED}Quell-Verbindung konnte nicht aufgebaut werden.')
                try:
                    target_connectable = job.target.check()
                    if target_connectable:
                        print(
                            f'\t{Fore.GREEN}Ziel-Verbindung konnte aufgebaut werden.\n')
                    else:
                        print(
                            f'\t{Fore.RED}Ziel-Verbindung konnte nicht aufgebaut werden.\n')
                except NotImplementedError:
                    print(
                        f'\t{Fore.YELLOW}Ziel unterstützt check() noch nicht.\n')
                except BaseException:
                    log.exception(
                        'Ziel-Verbindung konnte nicht getestet werden.')
                    print(
                        f'\t{Fore.RED}Ziel-Verbindung konnte nicht aufgebaut werden.')
                debug_result[job.name] = {
                    'creatable': True,
                    'source_connectable': source_connectable,
                    'target_connectable': target_connectable
                }
        # Beende Debug Lauf und setze Schrift-Farbe in der Shell zurück
        print(f'{Fore.RESET}Debug-Lauf beendet!\n')
        log.debug(f'========  Debug-Lauf für BRAXEL beendet. ========')
        return debug_result
