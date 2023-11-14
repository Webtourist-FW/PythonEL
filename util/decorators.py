""" Definiert verschiedene Decorator-Funktionen

    Functions:
        timeit
        timeit_async
"""

# IMPORTS
# Generelle Importe (Python Standard Module)
import logging as log
import time
from functools import wraps

# 3rd-Party Importe

# Eigene Module

# Set up Logger
log.getLogger(__name__).addHandler(log.NullHandler())

# FUNKTIONEN / KLASSEN


def timeit(f):
    """Decorator: Prints execution time after a function has run.
    """
    @wraps(f)
    def wrapper(*args, **kw):
        start = time.perf_counter()
        result = f(*args, **kw)
        end = time.perf_counter()
        time_string = f'function: {f.__name__} args: [{args}, {kw}] took: {end-start:2.6f} sec'
        log.debug(time_string)
        print(time_string)
        return result
    return wrapper


def timeit_async(f):
    """Decorator: Prints execution time after a function has run. Works with asynchronous functions.
    """
    @wraps(f)
    async def wrapper(*args, **kw):
        start = time.perf_counter()
        result = await f(*args, **kw)
        end = time.perf_counter()
        time_string = f'function: {f.__name__} args: [{args}, {kw}] took: {end-start:2.6f} sec'
        log.debug(time_string)
        print(time_string)
        return result
    return wrapper
