from pathlib import Path
from typing import List, Optional, TypeVar, Generic

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from ..interfaces.abstract_storage_manager import AbstractStorageManager 
from .with_dataset_converter import WithDatasetConverter


class SparkStorageManager(WithDatasetConverter, AbstractStorageManager[DataFrame]):
    """
    Uses Pandas as the underlying storage. Note that this storage manager
    will only work when mixed together with a resource manager.
    """
    def __init__(self, spark: Optional[SparkSession] = None, **kwargs):
        print(f"{self.__class__.__name__} - init")
        super().__init__(**kwargs)
        self._spark = spark or SparkSession.builder.getOrCreate()
        
        sc = self._spark.sparkContext
        self._hadoopPath = sc._gateway.jvm.org.apache.hadoop.fs.Path
        jFileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
        self._hadoopConf = sc._jsc.hadoopConfiguration()
        self._hdfs = jFileSystem.get(self._hadoopConf)

    def exists(self, key: str) -> bool:
        """Checks if the resource with the given key exists in the underlying storage"""
        if not self.has(key):
            self.logger.debug(f"Resource with key '{key}' is not defined in the resource manager")
        
        dataset_path = self.resolve(key)

        return self._hdfs.exists(self._hadoopPath(dataset_path))

    def load(self, key: str, **kwargs) -> DataFrame:
        """Loads a resource from the underlying storage"""
        resource = self.resource(key)

        path = self.resolve(key)
        file_type = resource.get_string("type", default=self.default_file_type)
        options = resource.get("options", default=dict())

        reader = self._spark.read.format(file_type)

        if any(options):
            reader = reader.options(**options)

        print(f"Reading dataset from path '{path}'")
        df = reader.load(path)

        return df

    def save(self, key: str, dataset: DataFrame, overwrite: Optional[bool] = True, partition_by: Optional[List[str]] = None, **kwargs) -> bool:
        """Saves a dataset to the underlying storage"""
        print(f"{self.__class__.__name__} - save")
        resource = self.resource(key)
        path = self.resolve(key)
        file_type = resource.get("type", default=self.default_file_type)
        options = resource.get("options", default=dict())

        self.logger.debug(f"Saving dataset '{key}' at path '{path}'")

        # This will convert the dataset to pandas, assuming we have a converter for it
        dataset = self._convert(dataset, DataFrame)
        mode = "overwrite" if overwrite else "error"
        
        writer = dataset.write.mode(mode).format(file_type)

        if partition_by:
            self.logger.debug(f"Partitioning dataset '{key}' by columns '{partition_by}'")
            writer = writer.partitionBy(partition_by)
        
        writer.save(path)
        
        return self.exists(key)

    def delete(self, key: str, **kwargs) -> bool:
        """Deletes a dataset from the underlying storage"""
        path = self.resolve(key)
        self.logger.debug(f"Deleting dataset '{key}' at path '{path}'")
        
        return self._hdfs.delete(self._hadoopPath(path), True)
