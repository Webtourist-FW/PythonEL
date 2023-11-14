
""" BRAXEL Hauptprogramm. Kümmert sich um Kommandozeilen-Argumente und Logging.

    Braxel Grundlogik steht in braxel.braxel
    Quell-Definitionen stehen in braxel.sources
    Ziel-Definitionen stehen in braxel.targets
    Job-Definitionen stehen in braxel.job
    Utility Funktionalitäten stehen in braxel.util
    Umgebungsvariablen werden in braxel.environment definiert

    Functions:
        setup_logger(name, path)
        parse_args()
        main()

    Usage:
        Bei aktivierte Python-Umgebung: braxel.py --job JOBNAME --parallel
        Mit Poetry: poetry run braxel --job JOBNAME --parallel
"""

# IMPORTS
# Generelle Importe (Python Standard Module)
import argparse
from datetime import datetime
import logging as log
from pathlib import Path

# 3rd-Party Importe

# Eigene Module
from braxel.environment import BraxelEnvironment
from braxel.braxel import Braxel

# FUNKTIONEN / KLASSEN


def setup_logger(name: str, path: str | Path, level: int = log.INFO) -> None:
    """Initialisiere den Logger (filename, log level, format, ...)

    Args:
        name (str): Name des Programms. Wird im Namen der Logdatei genutzt.
        path (str | Path): Pfad zum Log-Verzeichnis.
        level (int, optional): Log-Level. Defaults to log.INFO
    """
    logpath = Path(path)
    if not logpath.exists():
        logpath.mkdir(parents=True)
    current_time = datetime.now().strftime("%Y-%m-%d")
    filename = f'{current_time} - {name}.log'
    log.basicConfig(
        filename=logpath /
        filename,
        filemode='a',
        format='T: %(asctime)s :L: %(levelname)s :V: %(filename)s:%(lineno)s:%(funcName)s :N: %(message)s',
        level=level)

    log.info(f'==========  {name} gestartet.  ==========')


def parse_args() -> argparse.Namespace:
    """Verarbeite Komandozeilen-Argumente und gebe sie als Objekt zurück

    Returns:
        argparse.Namespace: Namespace-Objekt mit Kommandozeilen-Argumenten
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--job',
        type=str,
        help='Führe nur den benannten Job aus.')
    parser.add_argument(
        '--parallel',
        action='store_true',
        help='Lasse Jobs parallel laufen. (Achtung! Jeder Job muss eine andere Zieldatei/-tabelle haben.)')
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Initialisiere BRAXEL und prüfe die Konfiguration. Keine Jobs werden ausgeführt!')
    parser.add_argument(
        '--tag',
        type=str,
        help='Führe alle Jobs zu dem benannten Tag aus.')
    return parser.parse_args()


def main():
    """Lädt Braxel-Umgebung, startet Logging, verarbeitet Kommandozeilen-Argumente und führt Jobs aus.
        Erwartet Pflege von Konfiguration über YAML-Dateien, die per Umgebungsvariablen verlinkt sind.
    """
    # Lade Umgebungsvariablen
    env = BraxelEnvironment()
    # Hole die Kommandozeilen Argumente
    args = parse_args()
    if args.debug:
        # Starte Logging (wenn Log-Pfad konfiguriert) mit Log-Level DEBUG
        if env.logpath():
            setup_logger('BRAXEL', env.logpath(), level=log.DEBUG)
        try:
            Braxel.debug(job_name=args.job, tag_name=args.tag)
        except BaseException:
            log.exception('BRAXEL Debug ist fehlgeschlagen.')
    else:
        # Starte Logging (wenn Log-Pfad konfiguriert) mit Log-Level INFO
        if env.logpath():
            setup_logger('BRAXEL', env.logpath(), level=log.INFO)
        try:
            braxel = Braxel()
            braxel.run(job_name=args.job,tag_name=args.tag, run_parallel=args.parallel)
        except BaseException:
            log.exception('BRAXEL Run wegen Fehler abgebrochen.')


# PROGRAMM
if __name__ == '__main__':
    main()
