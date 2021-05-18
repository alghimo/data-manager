from typing import Optional

from ..interfaces.data_manager import DataManager
from ..mixins.spark_storage_manager import SparkStorageManager
from ..mixins.configured_resource_manager import ConfiguredResourceManager
from ..mixins.logger import Logger
from ..mixins.string_path_resolver import StringPathResolver


class SparkConfiguredDataManager(StringPathResolver, SparkStorageManager, ConfiguredResourceManager, DataManager, Logger):
    @property
    def _config_root(self) -> Optional[str]:
        """"
        The config root, if set the subkey to use as the base config for the resources.
        This can be useful if you want to have multiple resource types within the
        same config tree.
        """
        return "data.hdfs"
    
    @property
    def id(self) -> str:
        return "spark-configured"