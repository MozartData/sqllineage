import logging.config

NAME = "sqllineage"
VERSION = "100.0.5a"
DEFAULT_LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {"default": {"format": "%(levelname)s: %(message)s"}},
    "handlers": {
        "console": {
            "level": "WARNING",
            "class": "logging.StreamHandler",
            "formatter": "default",
        }
    },
    "loggers": {
        "": {
            "handlers": ["console"],
            "level": "WARNING",
            "propagate": False,
            "filters": [],
        },
        "werkzeug": {
            "handlers": ["console"],
            "level": "ERROR",
            "propagate": False,
            "filters": [],
        },
    },
}
logging.config.dictConfig(DEFAULT_LOGGING)

STATIC_FOLDER = "build"
DEFAULT_PORT = 5000
