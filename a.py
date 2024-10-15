for each in [
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
    "TimestampType",
    "TimestampNTZType",
    "NullType",
]:
    print(f"\"{each[:-4].lower()}\": DataType.{each},")