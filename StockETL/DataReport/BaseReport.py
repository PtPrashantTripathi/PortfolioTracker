import json
from typing import Optional

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pydeequ.profiles import ColumnProfilerRunner, StandardColumnProfile
from pyspark.sql.types import (
    NullType,
    DoubleType,
    StringType,
    StructType,
    BooleanType,
    IntegerType,
    StructField,
)

from StockETL.Common.CustomLogger import get_logger, with_logging
from StockETL.DataContract.Contract import Contract

logger = get_logger(name="StockETL - REPORTS")


class DataReport:
    """Base Model for Data Reports"""

    def __init__(
        self,
        input_df: DataFrame,
        transform_df: DataFrame,
        error_df: DataFrame,
        output_df: DataFrame,
        meta_data: dict,
        contract: Contract,
        spark: SparkSession,
        status: Optional[bool] = True,
    ) -> None:
        self.input_df = input_df
        self.transform_df = transform_df
        self.error_df = error_df
        self.output_df = output_df
        self.meta_data = meta_data
        self.contract = contract
        self.spark = spark
        self.status = "SUCCESS" if status else "FAILED"

    def get_size_count(self, count_type: str):
        """Get size"""
        factory = {
            "duplicated": self.error_df.filter(
                'array_contains(row_status_reason, "Duplication within batch ingestions")'
            ),
            "notqualified": self.error_df.filter(
                'not array_contains(row_status_reason, "Duplication within batch ingestions")'
            ),
            "input": self.input_df,
            "transform": self.transform_df,
            "output": self.output_df,
        }
        return factory.get(count_type).count()

    @with_logging(logger_module=logger)
    def add_runtime(self) -> dict:
        """Runtime Report"""
        self.payload = {
            "RAW": self.get_size_count("input"),
            "TRANSFORM": self.get_size_count("transform"),
            "DUPLICATION": self.get_size_count("duplicated"),
            "NOTQUALIFIED": self.get_size_count("notqualified"),
            "LOAD": self.get_size_count("output"),
        }
        return self.payload

    def _create_report_DataFrame(
        self, report_type: str, data_df: pd.DataFrame, struct_type: StructType
    ):
        """Create empty Dataframe using data frame from pyspark"""
        empty_dataframe = self.spark.createDataFrame(
            data_df[self.meta_data["reports"][report_type]], schema=struct_type
        )
        return empty_dataframe

    def precheck_data_quality(self) -> DataFrame:
        """prepare data for data quality"""
        struct_type = StructType(
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
        data = [
            {
                "created": self.meta_data["landing_date"],
                "layer": self.meta_data["layer"],
                "stage": self.meta_data["stage"],
                "data_source": self.meta_data["data_source"],
                "object_name": self.meta_data["object_name"],
                "uuid": str(self.meta_data["job_id"]),
                "file_ingest": self.meta_data["file_ingest"],
                "status": self.status,
            }
        ]
        df = pd.read_json(json.dumps(data))
        df["duplicated"] = int(self.payload["DUPLICATION"])
        df["notqualified"] = int(self.payload["NOTQUALIFIED"])
        df["raw"] = int(self.payload["RAW"])
        df["transformed"] = int(self.payload["TRANSFORM"])
        df["load"] = int(self.payload["LOAD"])
        df["YYYY"] = self.meta_data["landing"]["year"]
        df["MM"] = self.meta_data["landing"]["month"]
        df["DD"] = self.meta_data["landing"]["day"]
        return self._create_report_DataFrame(
            report_type="data_quality_columns", data_df=df, struct_type=struct_type
        )

    @with_logging(logger_module=logger)
    def add_data_quality(self):
        """Quality Report"""
        if len(self.output_df.take(1)) != 0:
            self.data_quality_df = self.precheck_data_quality()
        else:
            logger.info("Skipping empty dataframe")
            return None

    def precheck_data_catalog(self) -> DataFrame:
        """Precheck Data Catalog"""
        struct_type = StructType(
            [
                StructField("created", StringType(), True),
                StructField("layer", StringType(), True),
                StructField("stage", StringType(), True),
                StructField("data_source", StringType(), True),
                StructField("object_name", StringType(), True),
                StructField("field_name", StringType(), True),
                StructField("data_type", StringType(), True),
                StructField("field_description", StringType(), True),
                StructField("field_validations", StringType(), True),
                StructField("field_primary", BooleanType(), True),
                StructField("nullable", StringType(), True),
                StructField("completeness", StringType(), True),
                StructField("distinct_value", StringType(), True),
                StructField("mean", DoubleType(), True),
                StructField("max", DoubleType(), True),
                StructField("min", DoubleType(), True),
                StructField("sum", DoubleType(), True),
                StructField("stdDev", DoubleType(), True),
                StructField("histogram", StringType(), True),
                StructField("uid", StringType(), True),
                StructField("file_ingest", StringType(), True),
                StructField("YYYY", StringType(), True),
                StructField("MM", StringType(), True),
                StructField("DD", StringType(), True),
            ]
        )
        result = ColumnProfilerRunner(self.spark).onData(self.output_df).run()
        list_result = []
        for col, p in zip(result.profiles, result.profiles.items()):
            col_dict = {
                "field_name_c": col.lower(),
                "dataType": p[1].dataType,
                "Nullable": "TRUE",
                "completeness": p[1]._completeness,
                "Distinct Value": p[1].approximateNumDistinctValues,
            }
            if isinstance(p[1], StandardColumnProfile):
                col_dict["Mean"] = None
                col_dict["Max"] = None
                col_dict["Min"] = None
                col_dict["Sum"] = None
                col_dict["StdDev"] = None
                col_dict["histogram"] = p[1].typeCounts
            else:
                col_dict["Mean"] = p[1]._mean
                col_dict["Max"] = p[1]._maximum
                col_dict["Min"] = p[1]._minimum
                col_dict["Sum"] = p[1]._sum
                col_dict["StdDev"] = p[1]._stdDev
                col_dict["histogram"] = p[1].histogram
            list_result.append(col_dict)
        result_df = pd.read_json(json.dumps(list_result))
        for col in ["Mean", "Max", "Min", "Sum", "StdDev"]:
            result_df[col] = result_df[col].astype(float)
        result_df["created"] = self.meta_data["landing_date"]
        result_df["data_source"] = self.meta_data["data_source"]
        result_df["object_name"] = self.meta_data["object_name"]
        result_df["uid"] = self.meta_data["job_id"]
        result_df["YYYY"] = self.meta_data["landing"]["year"]
        result_df["MM"] = self.meta_data["landing"]["month"]
        result_df["DD"] = self.meta_data["landing"]["day"]
        result_df["file_ingest"] = self.meta_data["file_ingest"]
        result_df["layer"] = self.meta_data["layer"]
        result_df["stage"] = self.meta_data["stage"]

        contract_df = pd.json_normalize(
            self.contract.dict(),
            "contract_fields",
            [
                "contract_layer",
                "contract_stage",
                "contract_data_source",
                "contract_object_name",
                "contract_name",
                "contract_description",
            ],
        )
        result_df = result_df.merge(
            contract_df,
            how="left",
            left_on=[
                "data_source",
                "object_name",
                "layer",
                "stage",
                "field_name_c",
            ],
            right_on=[
                "contract_data_source",
                "contract_object_name",
                "contract_layer",
                "contract_stage",
                "field_name",
            ],
        )
        # print(result_df.info())
        result_df["field_is_primary"] = (
            result_df["field_is_primary"].fillna(0).astype("bool")
        )
        return self._create_report_DataFrame(
            report_type="data_catalog_columns",
            data_df=result_df,
            struct_type=struct_type,
        )

    @with_logging(logger_module=logger)
    def add_data_catalog(self):
        """Catalog Report"""
        if len(self.output_df.take(1)) != 0:
            self.data_catalog_df = self.precheck_data_catalog()
        else:
            logger.info("Skipping empty dataframe")
            return None

    @with_logging(logger_module=logger)
    def generate_invalid_records(self):
        """Write invalid records into parquet file, location /mnt/DataQuality"""
        output_schema = self.error_df.schema
        self.error_df = self.error_df.selectExpr(
            *[
                (
                    f"CAST({field.name} AS String) {field.name}"
                    if isinstance(field.dataType, NullType)
                    else f"{field.name}"
                )
                for field in output_schema
            ]
        )
        self.error_df.write.format("parquet").mode("overwrite").option(
            "mergeSchema", True
        ).save(self.meta_data["path"]["data_quality_invalid_path"])

    @with_logging(logger_module=logger)
    def commit(self):
        """Commit Data Report into DB"""
        # Generate Invalid records to parquet
        if len(self.error_df.take(1)) != 0:
            self.generate_invalid_records()
        else:
            logger.info("No Error Records found - Skipping write invalid records")
        if len(self.transform_df.take(1)) != 0:
            # Data Quality report
            self.data_quality_df.write.format("delta").mode("append").option(
                "mergeSchema", True
            ).partitionBy(self.meta_data["reports"]["report_partition_by"]).save(
                self.meta_data["path"]["data_quality"]
            )
            data_quality_message = (
                f"END: QUALITY : Generated with count as : {self.payload}"
            )
            # Data Catalog report
            self.data_catalog_df.write.format("delta").mode("append").option(
                "mergeSchema", True
            ).partitionBy(self.meta_data["reports"]["report_partition_by"]).save(
                self.meta_data["path"]["data_catalog"]
            )
            data_catalog_message = f"END: PROFILER: Generate {self.data_catalog_df.count()} rows match with {len(self.data_catalog_df.schema.names)} columns"
        else:
            data_quality_message = (
                f"END: QUALITY : Skipping Data Quality report, Empty Loaded"
            )
            data_catalog_message = (
                f"END: PROFILER: Skipping Data Catalog report, Empty loaded"
            )
        logger.info(data_quality_message)
        logger.info(data_catalog_message)
