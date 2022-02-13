import logging
from typing import Optional


class Logger:
    def __init__(self, logger_name: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)
        logger_name = logger_name or self.__class__.__name__
        
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.DEBUG)
