from pathlib import Path
from typing import Optional

from ..interfaces.abstract_resource_resolver import AbstractResourceResolver


class StringPathResolver(AbstractResourceResolver[str]):
    _DEFAULT_FILE_TYPE: str = "parquet"
    
    def __init__(self, default_file_type: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)
        self._default_file_type = default_file_type or self._DEFAULT_FILE_TYPE
    
    @property
    def default_file_type(self) -> str:
        return self._default_file_type

    def resolve(self, key: str) -> str:
        return self.resource(key).get_string("path")
