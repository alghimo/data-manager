from typing import Any, Callable, Dict, Optional, Tuple, Type

from pandas import DataFrame as PandasDataFrame
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession

from ..interfaces.abstract_dataset_converter import AbstractDatasetConverter


TYPE_IN = TYPE_OUT = Type
CONVERTER_KEY = Tuple[TYPE_IN , TYPE_OUT]
CONVERTER_FUNCTION = Callable[[TYPE_IN], TYPE_OUT]

class DatasetConverter(AbstractDatasetConverter):
    """
    The keys here are tuples of the pairs we want to be able to convert automatically.
    The values are the functions that, given two types, converts one into the other.
    For instance, given classes A and B, and a function ```convert(from: A, to: B) -> B```,
    ```_converters``` could be:

    _converters = {
        (A, B): convert
    }
    """
    def __init__(self, converters: Optional[Dict[CONVERTER_KEY, CONVERTER_FUNCTION]] = None, spark: Optional[SparkSession] = None):
        self._converters = converters or dict()
        self._spark = spark or SparkSession.builder.getOrCreate()

        if converters is None:
            self.register(PandasDataFrame, SparkDataFrame, self.pandas_to_spark)
            self.register(SparkDataFrame, PandasDataFrame, self.spark_to_pandas)

    def register(self, from_type: TYPE_IN, to_type: TYPE_OUT, converter: CONVERTER_FUNCTION):
        self._converters[(from_type, to_type)] = converter
    
    def convert(self, dataset: Any, to_type: TYPE_OUT) -> TYPE_OUT:
        from_type = type(dataset)
        # No conversion needed
        if from_type == to_type:
            return dataset
        
        key = (from_type, to_type)

        if key not in self._converters:
            raise ValueError(f"There is no converter registerd to go from class {from_type} to class {to_type}")
        
        converter = self._converters[key]

        return converter(dataset)
    
    def spark_to_pandas(self, spark_df: SparkDataFrame) -> PandasDataFrame:
        return spark_df.toPandas()
    
    def pandas_to_spark(self, pandas_df: PandasDataFrame) -> SparkDataFrame:
        self._spark = SparkSession.builder.getOrCreate()

        return self._spark.createDataFrame(pandas_df)


