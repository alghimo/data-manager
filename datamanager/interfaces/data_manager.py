from abc import ABC, abstractmethod

from .abstract_storage_manager import AbstractStorageManager
from .abstract_resource_manager import AbstractResourceManager
from .abstract_resource_resolver import AbstractResourceResolver


class DataManager(AbstractResourceResolver, AbstractStorageManager, AbstractResourceManager, ABC):
    """
    A DataManager is the combination of:
    - A ResourceManager, that handles the configuration of the datasets
    - A StorageManager, the takes care of interactions with the underlying storage
    """
    @property
    @abstractmethod
    def id(self) -> str:
        pass
