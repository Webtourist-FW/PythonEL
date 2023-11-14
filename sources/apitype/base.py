""" Definiert den grundsätzlichen Umgang mit API-Quellen.

    Classes:
        APISource
"""

# IMPORTS
# Generelle Importe (Python Standard Module)
import logging as log

# 3rd-Party Importe
import re
from urllib.parse import urljoin, urlencode

# Eigene Module
from braxel.sources.base import Source

# Set up Logger
log.getLogger(__name__).addHandler(log.NullHandler())


# FUNKTIONEN / KLASSEN
class APISource(Source):
    """Basis-Klasse für API-Quellen.

    Inheritance:
        Source: Erbt von der Basis-Quelle

    Attributes:
        name: Name der Quelle
        type: Typ der Quelle
        baseurl: Basis Url (Protokoll + Domain + Port), z.B. https://www.brax.com/
        path: Pfad der URL (wird an die Base-Url angefügt; kann Variablen enthalten), z.B: /api/test/{product}
        urlvars: Dictionary aller URL-Variablen, die noch in den URL-Pfad eingefügt werden sollen
        parameters: Dictionary mit allen URL-Parametern
    """

    def __init__(
            self,
            name: str,
            type: str,
            baseurl: str,
            path: str = '',
            urlvars: dict | None = None,
            parameters: dict | None = None) -> None:
        super().__init__(name, type)
        self.baseurl = baseurl
        self.urlvars = urlvars
        self.path = self._parse_path(path)
        self.parameters = parameters

    def _parse_path(self, path) -> str:
        """Liest Variablen aus einem Pfad aus und fügt die Werte ein (siehe self.urlvars).

        Args:
            path (_type_): Pfad der URL mit oder ohne Variablen

        Returns:
            str: Pfad, in dem alle Variablen ersetzt wurden
        """
        parts = path.split('/')
        parts_replaced = [self._replace_urlvar(p) for p in parts]
        path_new = '/'.join(parts_replaced)
        return path_new

    def _replace_urlvar(self, part) -> str:
        """Ersetzt eine URL-Variable durch ihren Wert. Bausteine, die keine Variablen sind, werden unverarbeitet zurückgegeben.

        Args:
            part (_type_): Einzelner Abschnitt des Pfades

        Raises:
            KeyError: Wenn die angegebene URL-Variable nicht in urlvars gefunden wurde.

        Returns:
            str: Wert der URL-Variablen oder Original-Baustein, falls keine Variable enthalten
        """
        pattern = re.compile(r'^{(?P<urlvar>[a-zA-Z0-9_]+)}$')
        match = pattern.match(part)
        if match:
            key = match.group('urlvar')
            value = self.urlvars.get(key)
            if not value:
                raise KeyError(
                    f'Die angegebene URL-Variable wurde nicht gefunden: {key}')
            return value
        else:
            return part

    def url(self, with_params: bool = False) -> str:
        """Erstellt fertige URL der API-Quelle.

        Args:
            with_params (bool, optional): Wenn True, werden die Parameter mit an die URL angehängt. Defaults to False.

        Returns:
            str: Vollständige URL der API-Quelle mit/ohne Parameter.
        """
        if with_params and self.parameters:
            return f'{urljoin(self.baseurl, self.path)}?{urlencode(self.parameters)}'
        return urljoin(self.baseurl, self.path)
