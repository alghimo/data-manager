from typing import Any, Dict, List, Optional

from pyhocon import ConfigTree

from ..interfaces.data_manager import DataManager
from ..mixins.logger import Logger
from ..types import DataContext


class CompositeDataManager(DataManager, Logger):
    """
    A composite data manager combines multiple other data managers, and also defines
    a priority no these data managers. This allows, for instance, to combine data managers
    that work based on configuration files, with others that work e.g. on directories.
    """
    def __init__(self, data_managers: Dict[str, DataManager],
                 data_manager_order: List[str] = None, **kwargs):
        super().__init__(**kwargs)
        self._data_managers = data_managers
        self._data_manager_order = data_manager_order or list(data_managers.keys())

    @property
    def id(self) -> str:
        return "composite(" + ','.join(self._data_manager_order) + ")"

    @property
    def resources(self) -> Dict[str, ConfigTree]:
        """Returns a list of all resources that the data manager has"""
        resources = {}

        for dm_id, dm in self._data_managers.items():
            self.logger.debug(f"Adding resources from data manager {dm_id}")
            resources = {**resources, **dm.resources}

        return resources

    def resource(self, key: str, data_manager_key: Optional[str] = None, **kwargs) -> ConfigTree:
        """Returns the configuration linked to a resource"""
        data_manager = self._get_data_manager(key=key, data_manager_key=data_manager_key)
        
        return data_manager.resource(key=key, **kwargs)

    def _available_data_managers(self, key: str, context: Optional[DataContext] = None) -> Dict[str, DataManager]:
        self.logger.debug(f"Getting available data managers for key '{key}'")
        available_dms = {dm_key: dm for dm_key, dm in self._data_managers.items() if dm.has(key, context)}
        num_dms = len(available_dms)
        str_dm = "data manager" if num_dms == 1 else "data managers"
        self.logger.debug(f"Found {str_dm} available for resource '{key}': [{', '.join(available_dms.keys())}]")

        return available_dms

    def _get_data_manager(self, key: str, context: Optional[DataContext] = None, data_manager_key: Optional[str] = None) -> DataManager:
        if not data_manager_key:
            self.logger.debug(f"Didn't get a data manager for resource '{key}', trying to determine one")
            available_dms = self._available_data_managers(key, context)
            for data_manager_key in self._data_manager_order:
                if data_manager_key in available_dms:
                    self.logger.debug(f"Using data manager '{data_manager_key}' for resource '{key}'")
                    return available_dms[data_manager_key]
            
            raise RuntimeError(f"No data manager found for resource '{key}'")

        return self._data_managers[data_manager_key]

    def resolve(self, key: str, data_manager_key: Optional[str] = None) -> str:
        """Resolves the resource name for the resource with the given key.

        Given the key, you will get the fully qualified table name / path to the resource, which will be different
        depending on the data manager.
        """
        data_manager = self._get_data_manager(key, data_manager_key=data_manager_key)

        return data_manager.resolve(key)

    def exists(self, key: str, data_manager_key: str = None) -> bool:
        """This method is similar to :func:`DataManager.has`, but it will also check if resource actually
        exists in the underlying data store.
        """
        has_resource = self.has(key, data_manager_key=data_manager_key)

        if not has_resource:
            return False
        
        data_manager = self._get_data_manager(key, data_manager_key=data_manager_key)
        
        return data_manager.exists(key)

    def load(self, key: str, data_manager_key: str = None, **kwargs) -> Optional[Any]:
        """Loads the dataset linked to the provided key."""
        data_manager = self._get_data_manager(key=key, context=DataContext.READ, data_manager_key=data_manager_key)

        return data_manager.load(key=key, **kwargs)

    def save(self, key: str, dataset: Any, overwrite: Optional[bool] = True, data_manager_key: Optional[str] = None, **kwargs) -> bool:
        """Writes a dataset to the underlying data source."""
        data_manager = self._get_data_manager(key=key, context=DataContext.WRITE, data_manager_key=data_manager_key)

        return data_manager.save(key=key, dataset=dataset, overwrite=overwrite, **kwargs)

    def delete(self, key: str, data_manager_key: Optional[str] = None, **kwargs) -> bool:
        """Deletes a resource from underlying data source."""
        data_manager = self._get_data_manager(key=key, context=DataContext.WRITE, data_manager_key=data_manager_key)

        return data_manager.delete(key=key, **kwargs)

    def has(self, key: str, context: Optional[DataContext] = None, data_manager_key: Optional[str] = None) -> bool:
        """Checks whether a resource is defined."""
        if not data_manager_key:
            for dm_key, dm in self._data_managers.items():
                if dm.has(key, context):
                    self.logger.debug(f"Data Manager {dm_key} has resource {key}")
                    return True
            return False

        return self._data_managers[data_manager_key].has(key, context)
