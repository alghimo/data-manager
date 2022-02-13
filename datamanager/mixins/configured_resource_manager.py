from typing import Dict, Optional

from pyhocon import ConfigTree

from ..interfaces.abstract_resource_manager import AbstractResourceManager
from ..types import DataContext


class ConfiguredResourceManager(AbstractResourceManager[ConfigTree]):
    def __init__(self, config: ConfigTree, **kwargs):
        super().__init__(**kwargs)
        self._config = config.get(self._config_root) if self._config_root is not None else config

    @property
    def _config_root(self) -> Optional[str]:
        """"
        The config root, if set the subkey to use as the base config for the resources.
        This can be useful if you want to have multiple resource types within the
        same config tree.
        """
        return None

    @property
    def resources(self) -> Dict[str, ConfigTree]:
        """
        Returns a dictionary with all resources, where keys are the dataset keys,
        and values are the dataset configurations.
        """
        return self._config

    def resource(self, key: str) -> ConfigTree:
        """Returns the configuration for a dataset"""
        return self._config.get(key)

    def add(self, key: str, resource: ConfigTree):
        """Adds a dataset configuration to the resources"""
        self._config.put(key, resource)
    
    def remove(self, key: str):
        """Removes a dataset configuration from the resources"""
        self._config.pop(key)

    def has(self, key: str, context: Optional[DataContext] = None) -> bool:
        """Checks if a resource with the given key is defined"""
        
        # If we don't have a config for the resource, return False
        if key not in self._config:
            return False
        
        """
        If we do have a config for the resource, we follow this logic:
        - If there is no context or the context is READ,
          return True (perhaps we want to implement a write-only in the future?) 
        - If the context is write and the resource is not read-only, we return true
        """
        resource = self.resource(key)

        if context is None or context == DataContext.READ:
            return True
        
        is_read_only = resource.get_bool("read_only", default=False)

        return not is_read_only


    
