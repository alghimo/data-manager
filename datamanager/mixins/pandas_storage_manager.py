from pathlib import Path
from typing import Optional, TypeVar, Generic

import pandas as pd

from ..interfaces.abstract_storage_manager import AbstractStorageManager 
from .with_dataset_converter import WithDatasetConverter


class PandasStorageManager(WithDatasetConverter, AbstractStorageManager[pd.DataFrame]):
    """
    Uses Pandas as the underlying storage. Note that this storage manager
    will only work when mixed together with a resource manager.
    """
    def exists(self, key: str) -> bool:
        """Checks if the resource with the given key exists in the underlying storage"""
        if not self.has(key):
            self.logger.debug(f"Resource with key '{key}' is not defined in the resource manager")
        
        dataset_path = self.resolve(key)

        return dataset_path.is_file()

    def load(self, key: str, **kwargs) -> pd.DataFrame:
        """Loads a resource from the underlying storage"""
        resource = self.resource(key)

        file_type = resource.get("type", default=self.default_file_type)
        read_method = f"read_{file_type.lower()}"
        if not hasattr(pd, read_method):
            error_msg = f"Pandas has no method '{read_method}' for file type '{file_type}'"
            raise ValueError(error_msg)

        reader = getattr(pd, read_method)
            
        dataset_path = self.resolve(key)
        options = resource.get("options", default=dict())

        self.logger.debug(f"Reading dataset '{key}' from path '{dataset_path}' using method '{read_method}' and options={dict(options)}'")
        
        return reader(dataset_path, **options)

    def save(self, key: str, dataset: pd.DataFrame, overwrite: Optional[bool] = True, **kwargs) -> bool:
        """Saves a dataset to the underlying storage"""
        dataset_path = self.resolve(key)

        if dataset_path.is_file() and not overwrite:
            raise ValueError(f"File '{dataset_path}' for dataset '{key}' already exists, and overwrite=False")
        
        resource = self.resource(key)
        if resource.get_bool("read_only", False):
            raise ValueError(f"Dataset '{key}' is read-only, you can't save to it!")

        file_type = resource.get("type", default=self.default_file_type)
        save_method = f"to_{file_type.lower()}"
        if not hasattr(pd, save_method):
            error_msg = f"Pandas has no method '{save_method}' for file type '{file_type}'"
            raise ValueError(error_msg)
        
        # This will convert the dataset to pandas, assuming we have a converter for it
        dataset = self._convert(dataset, pd.DataFrame)
        writer = getattr(dataset, save_method)
        
        # This shouldn't be a problem, but just in case, remember that get returns a ConfigTree, not a real dictionary.
        save_options = resource.get("save_options", default=dict())

        self.logger.debug(f"Writing dataset '{key}' to path '{dataset_path}' using method '{save_method}' and options={save_options}'")

        writer(dataset_path, **save_options)

        return dataset_path.is_file()

    def delete(self, key: str, **kwargs) -> bool:
        """Deletes a dataset from the underlying storage"""
        resource = self.resource(key)
        if resource.get_bool("read_only", False):
            raise ValueError(f"Dataset '{key}' is read-only, you can't delete it!")

        dataset_path = self.resolve(key)

        dataset_path.unlink()

        return not dataset_path.is_file()
