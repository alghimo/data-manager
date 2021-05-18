from enum import Enum, auto


class DataContext(Enum):
    READ = auto()
    WRITE = auto()