""" Definiert die von BRAXEL akzeptierten Umgebungsvariablen

    Classes:
        BraxelEnvironment
"""

# IMPORTS
# Generelle Importe (Python Standard Module)
import os
from pathlib import Path

# 3rd-Party Importe

# Eigene Module


# FUNKTIONEN / KLASSEN
class BraxelEnvironment:
    """Definiert die BRAXEL Umgebungsvariablen. Jede Umgebungsvariable hat eine Funktion, um diese auszulesen.

        Attributes:
            LOG_ENV
            JOB_ENV
            SOURCES_ENV
            TARGETS_ENV

        Methods:
            logpath()
            jobpath()
            sourcespath()
            targetspath()
    """

    def __init__(self) -> None:
        self.LOG_ENV = 'BRAXEL_LOG_PATH'
        self.JOB_ENV = 'BRAXEL_JOBS_CONFIG'
        self.SOURCES_ENV = 'BRAXEL_SOURCES_CONFIG'
        self.TARGETS_ENV = 'BRAXEL_TARGETS_CONFIG'

    def logpath(self) -> Path | None:
        if os.getenv(self.LOG_ENV):
            return Path(os.getenv(self.LOG_ENV))
        return None

    def jobpath(self) -> Path | None:
        if os.getenv(self.JOB_ENV):
            return Path(os.getenv(self.JOB_ENV))
        return None

    def sourcespath(self) -> Path | None:
        if os.getenv(self.SOURCES_ENV):
            return Path(os.getenv(self.SOURCES_ENV))
        return None

    def targetspath(self) -> Path | None:
        if os.getenv(self.TARGETS_ENV):
            return Path(os.getenv(self.TARGETS_ENV))
        return None
