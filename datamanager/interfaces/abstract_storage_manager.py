from abc import ABC, abstractmethod
from typing import TypeVar, Generic


T = TypeVar('T')

class AbstractStorageManager(Generic[T], ABC):
    """
    The storage manager is the one handling interactions with the actual storage,
    be it memory, files, databases, etc..
    Usually a storage manager will only work if mixed in together with a resource manager as well.
    """    
    @abstractmethod
    def exists(self, key: str) -> bool:
        """Checks if the resource with the given key exists in the underlying storage"""
        pass
    
    @abstractmethod
    def load(self, key: str, **kwargs) -> T:
        """Loads a resource from the underlying storage"""
        pass

    @abstractmethod
    def save(self, key: str, dataset: T, **kwargs) -> bool:
        """Saves a dataset to the underlying storage"""
        pass

    @abstractmethod
    def delete(self, key: str, **kwargs) -> bool:
        """Deletes a dataset from the underlying storage"""
        pass
