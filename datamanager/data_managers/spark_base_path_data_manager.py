from typing import Optional

from ..interfaces.data_manager import DataManager
from ..mixins.spark_storage_manager import SparkStorageManager
from ..mixins.base_path_resource_manager import BasePathResourceManager
from ..mixins.logger import Logger

from pyhocon import ConfigFactory
from pyspark.sql import DataFrame

from ..mixins.string_path_resolver import StringPathResolver


class SparkBasePathDataManager(StringPathResolver, SparkStorageManager, BasePathResourceManager, DataManager, Logger):
    @property
    def id(self) -> str:
        return "spark-base_path"

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
