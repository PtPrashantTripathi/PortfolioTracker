import os
from typing import Any, Callable
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from StockETL.Common import CustomSpark
from StockETL.Common.CustomLogger import get_logger, with_debug_logging

file_reader = Callable[[Any, Any], Any]

spark = CustomSpark.get_spark_session()
logger = get_logger("StockETL - FILEREADER")


class FileReader:
    """File Reader Base Class"""

    def __init__(self, input_data: Any, override_options: dict) -> None:
        if isinstance(input_data, str) and len(input_data.split(".")) == 1:  # folder
            file_list = os.listdir(f"/dbfs{input_data}")
            self.input_data = [f"{input_data}{file_name}" for file_name in file_list]
        else:
            self.input_data = input_data
        self.override_options = override_options
        self.ext = self.get_ext(self.input_data)
        self.options = self.build_override_options()
        self.file_format_spark = self.get_file_format_for_spark()

    @with_debug_logging(logger_module=logger)
    def get_ext(self, input_data: Any) -> str:
        """Return extension of the file or dataframe"""
        ext = None
        if isinstance(input_data, DataFrame):
            ext = "df"
        else:
            ext_pos = -2 if ".gz" in input_data else -1
            if isinstance(input_data, list):
                ext = input_data[0].split(".")[ext_pos]
            elif isinstance(input_data, str):
                ext = input_data.split(".")[ext_pos]
        logger.info(f"Extention options: {ext}")
        if not ext:
            raise Exception(f"Could not recognize file extension of {input_data}")
        return ext

    def build_override_options(self) -> dict:
        """Build override options"""
        if self.ext in ["df", "parquet", "delta"]:
            return None
        data = {
            key: self.override_options[key]
            for key in self.override_options
            if self.override_options[key] is not None
            and self.override_options[key] != "None"
        }
        if data.get("delimiter", None) is None:
            if self.ext == "csv":
                self.override_options["delimiter"] = ","
            if self.ext == "tsv":
                self.override_options["delimiter"] = "\t"
        return data

    def get_file_format_for_spark(self) -> str:
        """Get file format in spark.read.format('<format>')"""
        factory = {
            "xlsb": "excel",
            "xlsm": "excel",
            "xlsx": "excel",
            "xls": "excel",
            "csv": "csv",
            "tsv": "csv",
        }
        return factory.get(self.ext, self.ext)

    def get_builder(self):
        """Get builder"""
        builder = spark.read.format(self.file_format_spark)
        if self.options:
            logger.info(f"Adding Reader Override options{self.options}")
            for option in self.options:
                builder = builder.option(option, self.options[option])
        return builder

    def ingest_file(self, file_input=None):
        """Interface for load, check for single file or multiple file load or dataframe"""
        if not file_input:
            file_input = self.input_data
        if isinstance(file_input, DataFrame):
            return file_input
        self.builder = self.get_builder()
        if not isinstance(file_input, list):
            return self.builder.load(file_input)
        if self.ext in ["xlsx", "xls"]:
            map_object = map(self.builder.load, file_input)
            result = reduce(
                lambda x, y: x.unionByName(y, allowMissingColumns=True), map_object
            )
        elif self.ext == "parquet":
            result = self.builder.parquet(*file_input)
        elif self.ext in ["csv", "tsv"]:
            result = self.builder.load(file_input)
        else:
            result = self.builder.load(*file_input)
        return result

    def __repr__(self) -> str:
        return "File Reader"
