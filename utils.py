import dataclasses
import logging
from functools import wraps
from logging import getLogger
from time import perf_counter
from types import NoneType


def init_logger(name: str, logfile: str = "log/crawler.log"):
    """Initializes a logger with the given name (if it's not initialized),
    which writes onto the console and into a file.

    Parameters
    ----------
    name : str
        Name of the logger
    logfile : str, optional
        Path to the log file, by default "log/crawler.log"

    Returns
    -------
    logging.Logger
        The logger object
    """
    logger = logging.getLogger(name)
    if len(logger.handlers) == 0:
        logger.setLevel(logging.DEBUG)
        streamHandler = logging.StreamHandler()
        fileHandler = logging.FileHandler(logfile)
        fileHandler.setLevel(logging.DEBUG)
        streamHandler.setLevel(logging.DEBUG)
        errorFileHandler = logging.FileHandler(logfile.split(".")[0] + "_errors.json")
        errorFileHandler.setLevel(logging.ERROR)
        formatter = logging.Formatter(
            "%(asctime)s:%(name)s:%(levelname)s:%(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        fileHandler.setFormatter(formatter)
        streamHandler.setFormatter(formatter)
        errorFileHandler.setFormatter(formatter)
        logger.addHandler(streamHandler)
        logger.addHandler(fileHandler)
        logger.addHandler(errorFileHandler)
        return logger


def log_time(loggerName="root"):
    """Time logger decorator, logs how much time the operation took

    Parameters
    ----------
    loggerName : str, optional
        Name of the logger to log the time with, by default "root"

    """
    logger = getLogger(loggerName)

    def decorate(func):
        @wraps(func)
        def call(*args, **kwargs):
            start = perf_counter()
            result = func(*args, **kwargs)
            end = perf_counter()
            minutes = int((end - start) / 60)
            seconds = (end - start) - minutes * 60
            logger.info(
                "%s finished in %d:%0.2f seconds.",
                func.__name__,
                minutes,
                seconds,
            )
            return result

        return call

    return decorate


def to_dataclass(classtype, dictionary, fields=None):
    """Converts a dictionary into a dataclass object

    Parameters
    ----------
    classtype : class
        Type of the dataclass
    dictionary : _type_
        Dictionary which is converted
    fields : Tuple[dataclasses.Field], optional
        The fields to fill from the dictionary, by default None

    Returns
    -------
    _type_
        A dataclass object of the given type

    Raises
    ------
    KeyError
        If not all necessary fields can be found in the dictionary
    Exception
        If any other error occurs
    """
    if fields == None:
        fields = dataclasses.fields(classtype)

    fieldsDictionary = dictionary.get("fields", dict())

    obj = dict()
    for field in fields:
        if field.name in dictionary:
            element = dictionary[field.name]
        elif field.name in fieldsDictionary:
            element = fieldsDictionary[field.name]
        else:
            if (
                field.default == dataclasses.MISSING
                and field.default_factory == dataclasses.MISSING
            ):
                raise KeyError()
            continue

        if type(element) in (int, float, complex, str, bool, NoneType):
            obj[field.name] = element
        elif type(element) == dict:
            obj[field.name] = to_dataclass(field.type, element)
        elif type(element) == list:
            pass
        else:
            raise Exception(classtype, field.name, element, type(element))

    return classtype(**obj)


def handle_error(loggerName="root"):
    """Decorator to handle and log errors

    Parameters
    ----------
    loggerName : str, optional
        Name of the logger, by default "root"

    """
    logger = getLogger(loggerName)

    def decorate(func):
        @wraps(func)
        def call(*args, **kwargs):
            try:
                result = func(*args, **kwargs)
            except Exception as e:
                logger.error(
                    "Exception happened in %s with args %s %s.",
                    func.__name__,
                    repr(args),
                    repr(kwargs),
                    exc_info=1,
                )
            return result

        return call

    return decorate


def try_except(success, failure, exceptions=Exception):
    """Tries to call the function success
       if an exception occurs, calls the failure function

    Parameters
    ----------
    success : callable
        The function to try
    failure : Any
        Function to call or value to return when an exception was thrown
    exceptions : Exception, optional
        Exceptions to catch, by default Exception

    Returns
    -------
    _type_
        The result of success or failure
    """
    try:
        return success()
    except exceptions or Exception:
        return failure() if callable(failure) else failure
