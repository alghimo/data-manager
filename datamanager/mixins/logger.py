import logging
from typing import Optional


class Logger:
    def __init__(self, logger_name: Optional[str] = None, **kwargs):
        print(f"{self.__class__.__name__} - init")
        super().__init__(**kwargs)
        logger_name = logger_name or self.__class__.__name__
        print(f"creating logger {logger_name}")
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.DEBUG)
