import json

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from StockETL.Common import CustomSpark
from StockETL.Common.Functions import (
    Helper,
    StringType,
    StructType,
    IntegerType,
    StructField,
)
from StockETL.DataReport.BaseReport import DataReport

__doc__ = """Generate Data Quality and duplicate, non qualify"""
STRUCT_TYPE = StructType(
    [
        StructField("created", StringType(), True),
        StructField("layer", StringType(), True),
        StructField("stage", StringType(), True),
        StructField("data_source", StringType(), True),
        StructField("object_name", StringType(), True),
        StructField("status", StringType(), True),
        StructField("duplicated", IntegerType(), True),
        StructField("notqualified", IntegerType(), True),
        StructField("raw", IntegerType(), True),
        StructField("transformed", IntegerType(), True),
        StructField("load", IntegerType(), True),
        StructField("uid", StringType(), True),
        StructField("file_ingest", StringType(), True),
        StructField("YYYY", StringType(), True),
        StructField("MM", StringType(), True),
        StructField("DD", StringType(), True),
    ]
)


class DataQuality(DataReport):
    """Generate Data Quality"""

    def __init__(self, **data):
        super().__init__(**data)
        self.STRUCT_TYPE = STRUCT_TYPE

    def prepare_data(self, raw, transform, load, duplication, notqualify):
        """Prepare Data for dumping"""
        if isinstance(self.file_path, list):
            file_ingest = ",".join(self.file_path)
        elif isinstance(self.file_path, DataFrame):
            file_ingest = f"Custom DataFrame generated!"
        else:
            file_ingest = self.file_path
        data = [
            {
                "created": self.landing_date,
                "layer": self.layer,
                "stage": self.stage,
                "data_source": self.data_source,
                "object_name": self.object_name,
                "status": self.status,
                "uuid": self.uuid,
                "file_ingest": file_ingest,
            }
        ]
        df = pd.read_json(json.dumps(data))
        df["duplicated"] = int(duplication)
        df["notqualified"] = int(notqualify)
        df["raw"] = int(raw)
        df["transformed"] = int(transform)
        df["load"] = int(load)
        df["YYYY"] = Helper.Datetime.convert_string_to_date(self.landing_date, fmt="%Y")
        df["MM"] = Helper.Datetime.convert_string_to_date(self.landing_date, fmt="%m")
        df["DD"] = Helper.Datetime.convert_string_to_date(self.landing_date, fmt="%d")
        return self.spark.createDataFrame(
            df[self.meta_data["report"]["data_quality_columns"]],
            schema=self.STRUCT_TYPE,
        )

    def generate(self, raw, transform, load, duplication, notqualify):
        """Generate Data Quality"""
        data = self.prepare_data(
            raw=raw,
            transform=transform,
            load=load,
            duplication=duplication,
            notqualify=notqualify,
        )
        (
            data.write.format("delta")
            .mode("append")
            .option("mergeSchema", True)
            .partitionBy(self.meta_data["report"]["data_quality_partition_by"])
            .save(self.meta_data["path"]["data_quality"])
        )
        self.logger.log(
            f"""DATA DataQuality: QUALITY: Generated with count as : {raw} -{notqualify}- {transform}- {duplication}- {load}"""
        )

    def generate_invalid_record(self, error_df, table_name: str = ""):
        """Save Invalid record to file or table_name"""
        if error_df.count() == 0:
            return None
        error_df = error_df.withColumn("uuid", F.lit(self.uuid))
        if self.debug:
            self.logger.log(self.meta_data["path"]["data_quality_invalid_path"])
        if table_name == "":
            # Save as Path
            (
                error_df.write.format("delta")
                .mode("overwrite")
                .option("mergeSchema", True)
                .save(self.meta_data["path"]["data_quality_invalid_path"])
            )
        else:
            # Save as Table Delta
            (
                error_df.write.format("delta")
                .mode("overwrite")
                .option("mergeSchema", True)
                .saveAsTable(table_name)
            )
