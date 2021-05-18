from abc import ABC, abstractmethod
from typing import TypeVar, Generic

T = TypeVar("T")


class AbstractResourceResolver(Generic[T], ABC):
    """
    The resolver takes care of creating fully qualified names from resource keys.
    For instance, when working with files this would be the file path, when working
    with databases it will be the fully qualified table name, etc..
    """
    @abstractmethod
    def resolve(self, key: str) -> T:
        pass