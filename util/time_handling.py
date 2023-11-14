""" Definiert verschiedene Utility-Funktionen für den Umgang mit Zeiten und Datums-Werten

    Functions:
        parse_timedelta(delta_string)
        is_younger_than(path, date)
        get_datetime_from_timedelta(delta_string)
"""

# IMPORTS
# Generelle Importe (Python Standard Module)
import logging as log
from pathlib import Path
import re

# 3rd-Party Importe
import pendulum as pdl
from pendulum.datetime import DateTime

# Eigene Module

# Set up Logger
log.getLogger(__name__).addHandler(log.NullHandler())

# FUNKTIONEN / KLASSEN


def parse_timedelta(delta_string: str) -> dict:
    """Wertet ein Time-Delta als String aus und baut ein passendes Dictionary daraus, welches in Pendulum oder Datetime genutzt werden kann.

    Args:
        delta_string (str): Zeitunterschied (Timedelta) als String nach dem Format: 0y0m0w0d0H0M0S

    Raises:
        ValueError: Wirft Fehler, wenn der String nicht dem Format entspricht

    Returns:
        dict: Dictionary mit den einzelnen Zeit-Werten (Jahr, Monat, ...)
    """

    pattern = (
        r'^((?P<years>[0-9]*)y)?'
        r'((?P<months>[0-9]*)m)?'
        r'((?P<weeks>[0-9]*)w)?'
        r'((?P<days>[0-9]*)d)?'
        r'((?P<hours>[0-9]*)H)?'
        r'((?P<minutes>[0-9]*)M)?'
        r'((?P<seconds>[0-9]*)S)?$'
    )
    delta = re.match(pattern, delta_string)
    if not delta:
        raise ValueError(
            f'Delta String "{delta_string}", entspricht nicht definiertem Pattern: 0y0m0w0d0H0M0S')
    delta = {key: int(value) if value else 0 for key,
             value in delta.groupdict().items()}
    return delta


def is_younger_than(path: Path, date: DateTime) -> bool:
    """Prüft, ob ein Pfad jünger ist als das angegeben Datum (nutzt das Erstellungsdatum des Systems)

    Args:
        path (Path): Pfad der geprüft werden soll
        date (DateTime): Maximales Alter, als Datum

    Returns:
        bool: True, wenn der Pfad jünger ist. Ansonsten False.
    """

    if not date:
        return False
    creation_timestamp = path.stat().st_ctime
    creation_date = pdl.from_timestamp(creation_timestamp)
    return creation_date > date


def get_datetime_from_timedelta(delta_string: str) -> DateTime:
    """Wandelt ein Timedelta in ein Datum um, ausgehend vom heutigen Tag.

    Args:
        delta_string (str): Unterschied zum heutigen Tag als relative Zeitangabe nach dem Format: 0y0m0w0d0H0M0S

    Returns:
        DateTime: Datum des Tages von heute aus gerechnet
    """

    date = None
    if delta_string:
        try:
            timedelta = parse_timedelta(delta_string)
            date = pdl.now().subtract(**timedelta)
        except ValueError:
            log.exception(
                f'Datum konnte nicht aus Delta String: {delta_string} erstellt werden.')
    return date
