from pathlib import Path
from typing import Optional, Union

from pyhocon import ConfigTree

from .composite_data_manager import CompositeDataManager
from .pandas_configured_data_manager import PandasConfiguredDataManager
from .pandas_base_path_data_manager import PandasBasePathDataManager
from .spark_configured_data_manager import SparkConfiguredDataManager
from .spark_base_path_data_manager import SparkBasePathDataManager


class DataManagerFactory:
    @staticmethod
    def local(config: Optional[ConfigTree] = None, base_dir: Optional[Union[str, Path]] = None) -> CompositeDataManager:
        """
        Gives you a data manager a pandas configured data manager (which works with a configuration)
        and/or a pandas base_dir manager (which doesn't require configuration, simply a local directory).
        """
        if not config or base_dir:
            raise ValueError("You need to provide either a config or a base_dir")
        
        data_managers = dict()
        data_manager_order = []

        if config is not None:
            pd_configured_dm = PandasConfiguredDataManager(config)
            data_managers[pd_configured_dm.id] = pd_configured_dm
            data_manager_order.append(pd_configured_dm.id)

        if base_dir is not None:
            pd_dir_dm = PandasBasePathDataManager(base_dir=base_dir)
            data_managers[pd_dir_dm.id] = pd_dir_dm
            data_manager_order.append(pd_dir_dm.id)
        
        return CompositeDataManager(data_managers, data_manager_order)
    
    @staticmethod
    def all(config: Optional[ConfigTree] = None, local_base_dir: Optional[Union[str, Path]] = None, hdfs_base_dir: Optional[Union[str, Path]] = None) -> CompositeDataManager:
        """
        Gives you a data manager with one or more of the following data managers, with this order of priority:
        - If config is provided and has a "data.hdfs" tree: A SparkConfiguredDataManager is added
        - If config is provided and has a "data.local" tree: A PandasConfiguredDataManager is added
        - If hdfs_base_dir is provided or if config has an "hdfs_path" element: A SparkBasePathDataManager (which doesn't require config)
        - If local_base_dir is provided or if config has an "local_path" element: A PandasBasePathDataManager (which doesn't require config)

        Note that if both a SparkBasePathDataManager and a PandasBasePathDataManager, you will only be able to write
        to the pandas one if you explicitely provide the data manager key when saving.
        """

        if not config or local_base_dir or hdfs_base_dir:
            raise ValueError("You need to provide either a config or a base_dir")
        
        if local_base_dir is None and config.get_string("local_path", default=None) is not None:
            local_base_dir = config.get_string("local_path", default=None)

        if hdfs_base_dir is None and config.get_string("hdfs_path", default=None) is not None:
            hdfs_base_dir = config.get_string("hdfs_path", default=None)
        
        data_managers = dict()
        data_manager_order = []

        if config.get("data.hdfs", default=None) is not None:
            spark_configured_dm = SparkConfiguredDataManager(config=config)
            data_managers[spark_configured_dm.id] = spark_configured_dm
            data_manager_order.append(spark_configured_dm.id)

        if config.get("data.local", default=None) is not None:
            pd_configured_dm = PandasConfiguredDataManager(config=config)
            data_managers[pd_configured_dm.id] = pd_configured_dm
            data_manager_order.append(pd_configured_dm.id)

        if hdfs_base_dir is not None:
            spark_dir_dm = SparkBasePathDataManager(base_path=hdfs_base_dir)
            data_managers[spark_dir_dm.id] = spark_dir_dm
            data_manager_order.append(spark_dir_dm.id)

        # Note that if there is an hdfs base dir, this one will only be used when called explicitly
        if local_base_dir is not None:
            pd_dir_dm = PandasBasePathDataManager(base_path=local_base_dir)
            data_managers[pd_dir_dm.id] = pd_dir_dm
            data_manager_order.append(pd_dir_dm.id)
        
        print(f"data_managers: {data_managers}")
        print(f"data_manager order: {data_manager_order}")
        return CompositeDataManager(data_managers, data_manager_order)
