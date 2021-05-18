from abc import ABC, abstractmethod
from typing import Any, Type

TYPE_OUT = Type


class AbstractDatasetConverter(ABC):
    @abstractmethod
    def convert(self, dataset: Any, to_type: TYPE_OUT) -> TYPE_OUT:
        pass