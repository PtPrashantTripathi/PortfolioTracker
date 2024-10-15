import numpy as np
import warnings
from typing import Optional, Dict, Union, Type
from functools import lru_cache
from dataclasses import dataclass

__all__ = [
    "StringType",
    "CharType",
    "VarcharType",
    "BinaryType",
    "BooleanType",
    "DecimalType",
    "FloatType",
    "DoubleType",
    "ByteType",
    "ShortType",
    "IntegerType",
    "LongType",
    "DateType",
    "DatetimeType",
    "NullType",
    "DataType",
]

# Define basic data types using NumPy
StringType = np.str_
CharType = np.str_  # NumPy treats char as string
VarcharType = np.str_
BinaryType = np.bytes_
BooleanType = np.bool_
DecimalType = np.float64  # Using float64 as a substitute
FloatType = np.float32
DoubleType = np.float64
ByteType = np.int8
ShortType = np.int16
IntegerType = np.int32
LongType = np.int64
DateType = np.datetime64  # Date without time
DatetimeType = np.datetime64  # Datetime with time
NullType = np.void  # Represents null/None

# Factory mapping of string names to NumPy types
_DATATYPE_FACTORY: Dict[str, Union[np.dtype, Type[np.generic]]] = {
    "string": StringType,
    "char": CharType,
    "varchar": VarcharType,
    "binary": BinaryType,
    "boolean": BooleanType,
    "decimal": DecimalType,
    "float": FloatType,
    "double": DoubleType,
    "byte": ByteType,
    "short": ShortType,
    "integer": IntegerType,
    "long": LongType,
    "date": DateType,
    "datetime": DatetimeType,
    "null": NullType,
}


class DataTypeWarning(UserWarning):
    """Warning for unsupported or unexpected datatype usage."""

    pass


@dataclass(slots=True)
class DataType:
    """A class representing data types using NumPy types."""

    name: str
    dtype: np.dtype

    def __init__(
        self, name: str, dtype: Optional[Union[str, np.dtype, Type[np.generic]]] = None
    ):
        self.name = name.capitalize()

        if dtype is None:
            # If dtype is not provided, try to fetch from _DATATYPE_FACTORY
            self.dtype = self._get_dtype_from_factory(name)
        else:
            # If both name and dtype are provided, use those directly
            self.dtype = np.dtype(dtype)

    def __repr__(self) -> str:
        return f"DataType(name='{self.name}', dtype='{self.dtype.name}')"

    @staticmethod
    @lru_cache(maxsize=32)
    def _get_dtype_from_factory(value: str) -> np.dtype:
        """Fetch the dtype from the factory or return StringType if not found."""
        name = value.replace("type", "").strip().lower()
        dtype = _DATATYPE_FACTORY.get(name, StringType)

        if name not in _DATATYPE_FACTORY:
            # Issue a warning for unsupported data types
            warnings.warn(
                f"Datatype '{value}' is not supported",
                category=DataTypeWarning,
                stacklevel=2,
            )

        return np.dtype(dtype)


# Example usage
if __name__ == "__main__":
    print(DataType("unknown"))  # Will trigger a warning, default to StringType
    print(DataType("datetime"))
    print(DataType("custom", np.float32))  # Directly use provided dtype
