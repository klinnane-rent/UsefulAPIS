import functools
from enum import Enum


@functools.total_ordering
class BaseEnum(str, Enum):
    @classmethod
    def list(cls):
        return [element for element in cls]

    def __lt__(self, other):
        if isinstance(other, type(self)):
            return self.value < other.value
        else:
            raise TypeError(f"Must compare two objects of type {type(self).__class__}")

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return self.value == other.value
        else:
            raise TypeError(f"Must compare two objects of type {type(self).__class__}")

    def __hash__(self):
        return id(self)

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return str(self.value)
