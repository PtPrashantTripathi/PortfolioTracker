import enum

__doc__ = """Dictionary Files - contain all validation str constrain for user input"""


class UEnumMeta(enum.EnumMeta):
    def __contains__(cls, item):
        return item in [v.value for v in cls.__members__.values()]

class SaveMode(enum.Enum, metaclass=UEnumMeta):
    """SaveMode Dictionary

    This use to validate user input."""

    APPEND = "append"
    OVERWRITE = "overwrite"
    IGNORE = "ignore"
    ERROR = "error"
    UPSERT = "upsert"
    SCD = "scd"
    SNAPSHOT = "snapshot"


class Layer(enum.Enum, metaclass=UEnumMeta):
    """Layer Dictionary

    This use to validate user input."""

    BRONZE = "Bronze"
    SILVER = "Silver"
    GOLD = "Gold"
    PLATINUM = "Platinum"


class Stage(enum.Enum, metaclass=UEnumMeta):
    """Stage Dictionary

    This use to validate user input."""

    LANDED = "Landed"
    PROCESSED = "Processed"


class UdfLocation(enum.Enum, metaclass=UEnumMeta):
    # TODO: Come up with something for pre-extract. for now disable it for easier use
    # PRE_EXTRACT = "PRE_EXTRACT"
    EXTRACT = "EXTRACT"
    POST_EXTRACT = "POST_EXTRACT"
    PRE_TRANSFORM = "PRE_TRANSFORM"
    POST_TRANSFORM = "POST_TRANSFORM"
    PRE_LOAD = "PRE_LOAD"
    POST_LOAD = "POST_LOAD"


class ERROR_LEVEL(enum.IntEnum, metaclass=UEnumMeta):
    """Error Level Dictionary

    This is used to determine action with the error."""

    WARN = 10
    ERROR = 20
    CRITICAL = 30

    # WARN < ERROR < CRITICAL


class Environment(enum.Enum, metaclass=UEnumMeta):
    """Environemt Dictionary

    This use to validate user input."""

    DEV = "dev"
    QA = "qa"
    PROD = "prod"


class Metastore(enum.Enum, metaclass=UEnumMeta):
    """Metastore Dictionary"""

    HIVE = "hive"
    UNITY_CATALOG = "unity_catalog"
