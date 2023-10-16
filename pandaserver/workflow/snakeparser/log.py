__author__ = "retmas"

import logging
import logging.config
import logging.handlers


class Singleton(type):
    def __init__(cls, *args, **kwargs):
        super(Singleton, cls).__init__(*args, **kwargs)
        cls._instance = None

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instance


class Logger(object, metaclass=Singleton):
    _config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {"default": {"format": "[%(asctime)s] [%(process)d] [%(levelname)s] [%(module)s] [%(funcName)s:%(lineno)d] - %(message)s"}},
        "handlers": {
            "console": {
                "level": "DEBUG",
                "class": "logging.StreamHandler",
                "formatter": "default",
            }
        },
        "loggers": {"snakeparser.log": {"handlers": ["console"], "level": "INFO"}},
    }

    def __init__(self):
        logging.config.dictConfig(self._config)

    @staticmethod
    def set_level(level):
        Logger.get().setLevel(level)

    @staticmethod
    def set_log_file(filename, max_bytes=0, backup_count=0, remove_console=False):
        logger = Logger().get()
        handler = logging.handlers.RotatingFileHandler(filename, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8")
        handler.formatter = logger.handlers[0].formatter
        handler.level = logging.DEBUG
        logger.addHandler(handler)
        if remove_console:
            logger.removeHandler(logger.handlers[0])

    @staticmethod
    def get() -> logging.Logger:
        return logging.getLogger(__name__)
