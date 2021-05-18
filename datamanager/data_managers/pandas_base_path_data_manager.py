from pyhocon import ConfigFactory

from ..interfaces.data_manager import DataManager
from ..mixins.pandas_storage_manager import PandasStorageManager
from ..mixins.base_path_resource_manager import BasePathResourceManager
from ..mixins.logger import Logger
from ..mixins.string_path_resolver import StringPathResolver

class PandasBasePathDataManager(StringPathResolver, PandasStorageManager, BasePathResourceManager, Logger, DataManager):
    @property
    def id(self) -> str:
        return "pandas-base_path"

    def save(self, key: str, *args, **kwargs) -> bool:
        saved = super().save(*args, key=key, **kwargs)

        if saved:
            self.add(key)
        
        return saved
    
    def delete(self, key: str, **kwargs) -> bool:
        deleted = super().delete(key=key, **kwargs)

        if deleted:
            self.remove(key)
        
        return deleted
