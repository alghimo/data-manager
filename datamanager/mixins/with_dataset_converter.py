from typing import Optional

from ..utils.dataset_converter import DatasetConverter
from ..interfaces.abstract_dataset_converter import AbstractDatasetConverter


class WithDatasetConverter:
    def __init__(self, dataset_converter: Optional[AbstractDatasetConverter], **kwargs):
        super().__init__(**kwargs)
        self._dataset_converter = dataset_converter or DatasetConverter()

    def _convert(self, *args, **kwargs):
        return self._dataset_converter.convert(*args, **kwargs)