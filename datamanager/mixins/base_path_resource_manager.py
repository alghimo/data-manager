from pathlib import Path
from typing import Dict, Optional

from pyhocon import ConfigFactory, ConfigTree

from ..interfaces.abstract_resource_manager import AbstractResourceManager
from ..types import DataContext


class BasePathResourceManager(AbstractResourceManager[ConfigTree]):
    def __init__(self, base_path: str, **kwargs):
        super().__init__(**kwargs)
        self._base_path = Path(base_path)
        current_files = self._base_path.glob("*")
        resources = {
            f.name: f
            for f in current_files
        }
        self._resources = resources

    @property
    def base_path(self) -> str:
        return self._base_path
    
    @property
    def resources(self) -> Dict[str, str]:
        """
        Returns a dictionary with all resources, where keys are the dataset keys,
        and values are the dataset configurations.
        """
        return self._resources
    
    def _get_path(self, key: str) -> str:
        return str(self.base_path / f"{key}.{self.default_file_type}")

    def resource(self, key: str) -> ConfigTree:
        """Returns the configuration for a dataset"""
        return ConfigFactory.from_dict({"path": self._get_path(key)})

    def add(self, key: str, *args, **kwargs):
        """Adds a dataset configuration to the resources"""

        self._resources[key] = self.resource(key)
    
    def remove(self, key: str):
        """Removes a dataset configuration from the resources"""
        self._resources.pop(key, None)

    def has(self, key: str, context: Optional[DataContext] = None) -> bool:
        """Checks if a resource with the given key is defined"""
        
        # When writing, always say we do have the resource
        if context is not None and context == DataContext.WRITE:
            return True
        
        return key in self._resources
        