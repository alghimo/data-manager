from abc import ABC, abstractmethod
from typing import Dict, Generic, Optional, TypeVar

from ..types import DataContext

T = TypeVar('T')


class AbstractResourceManager(Generic[T], ABC):
    """
    The resource manager is responsible for handling the configuration of the datasets,
    associating the dataset keys to their configurations
    """
    @property
    @abstractmethod
    def resources(self) -> Dict[str, T]:
        """
        Returns a dictionary with all resources, where keys are the dataset keys,
        and values are the dataset configurations.
        """
        pass
    
    @abstractmethod
    def resource(self, key: str) -> T:
        """Returns the configuration for a dataset"""
        pass

    @abstractmethod
    def has(self, key: str, context: Optional[DataContext] = None) -> bool:
        """Checks if a resource with the given key is defined"""
        pass


    
    def add(self, key: str, resource: T):
        """Adds a dataset configuration to the resources"""
        raise NotImplementedError("This data manager does not support adding resources to it")
    
    def remove(self, key: str):
        """Removes a dataset configuration from the resources"""
        raise NotImplementedError("This data manager does not support removing resources from it")
