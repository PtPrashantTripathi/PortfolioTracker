from abc import ABC, abstractmethod
from typing import Any, ClassVar

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from delta.tables import *
from pyspark.storagelevel import StorageLevel

from StockETL.core_error import ExtractEmptyFileWithNoRecord
from StockETL.Common.Functions import get_current_timestamp, convert_string_to_date
from StockETL.Common.CustomLogger import get_logger, with_logging
from StockETL.DataContract.Contract import Contract
from StockETL.DataOnboarder.Processor.Validator import UValidator
from StockETL.DataOnboarder.Processor.FileReader import FileReader

logger = get_logger(name="StockETL - PROCESSOR")


class StockETLStoreLevel(StorageLevel):
    ADLS: ClassVar["StorageLevel"]


StockETLStoreLevel.ADLS = "ADLS"


class BaseProcessor(ABC):
    """Base Class Processor"""

    def __init__(
        self,
        spark: SparkSession,
        input_data: Any,
        reader: FileReader,
        custom_udf: list,
        meta_data: dict,
        contract: Contract,
        sequential_extraction: bool,
        cache_level: str,
    ) -> None:
        """Init"""
        self.input_data = input_data
        self.spark = spark
        self.reader = reader
        self.custom_udf = custom_udf
        self.meta_data = meta_data
        self.contract = contract
        self.sequential_extraction = sequential_extraction
        # define date for use across the processes
        self.landing_date = convert_string_to_date(
            self.meta_data["landing_date"], fmt="%Y-%m-%d"
        )
        self.current_timestamp = get_current_timestamp()
        self.cache_level = cache_level

    @with_logging(logger_module=logger)
    @abstractmethod
    def extract(self) -> DataFrame:
        """extract"""

    @with_logging(logger_module=logger)
    @abstractmethod
    def transform(self) -> DataFrame:
        """transform"""

    @with_logging(logger_module=logger)
    @abstractmethod
    def load(self) -> None:
        """load"""

    @with_logging(logger_module=logger)
    @abstractmethod
    def validate(self):
        """validate"""

    def build_generated_primary_keys(self, dataframe):
        transformed_df = dataframe
        if len(self.meta_data["schema"]["primary_keys"]) > 1:
            transformed_df = dataframe.withColumn(
                "RECID",
                F.concat_ws(
                    "_",
                    *[F.col(col) for col in self.meta_data["schema"]["primary_keys"]],
                ),
            )
            self.meta_data["schema"]["RECID"] = True
        return transformed_df

    def drop_generated_primary_keys(self, dataframe: DataFrame):
        result = dataframe
        if "RECID" in dataframe.schema.names:
            result = dataframe.drop("RECID")
        return result.repartition(1)

    def execute_udf_function(
        self, input_df: DataFrame, col_name: str, custom_func, params
    ) -> DataFrame:
        """Execute Custom UDF Function"""
        if params:
            return input_df.withColumn(col_name, custom_func())
        return input_df.withColumn(col_name, custom_func())

    def custom_udf_func(self, input_df: DataFrame, location: str) -> DataFrame:
        """Custom function"""
        if not self.custom_udf.get(location, None):
            return input_df
        for step in self.custom_udf[location]:
            message = f"EXECUTING UDF at {location} - chaing column {step['col_name']} - {step['function_name']}"
            logger.info(message)
            input_df = self.execute_udf_function(
                input_df, step["col_name"], step["function_name"], step["params"]
            )
        return input_df

    def get_validator(self, input_df):
        """Get validator for processor"""
        return UValidator(
            input_df=input_df,
            contract=self.contract,
            meta_data=self.meta_data,
        )

    def prepare_load_sql(self, write_mode):
        write_mode_dict = {
            "upsert": self.prepare_upsert_sql,
            "scd": self.prepare_scd_type_2_sql,
            "snapshot": self.prepare_snapshot_sql,
            "overwrite": self.prepare_overwrite_sql,
        }
        prepare_func = write_mode_dict.get(write_mode, None)
        if prepare_func == None:
            raise Exception(f"Not supported `{write_mode}` write mode!")
        prepare_func()

    @with_logging(logger_module=logger)
    def commit_sql(self, sql_string: str) -> None:
        """Wrapper for logging"""
        self.spark.sql(sql_string)

    def get_ddl_from_input_data(self, input_data):
        if isinstance(input_data, str):
            df = self.spark.read.format("delta").load(path=input_data)
        if isinstance(input_data, DataFrame):
            df = input_data
        ddl = self.spark.sparkContext._jvm.org.apache.spark.sql.types.DataType.fromJson(
            df.schema.json()
        ).toDDL()
        return ddl

    @with_logging(logger_module=logger)
    def check_addition_columns(self, source_schema: list) -> None:
        new_columns = list(set(self.meta_data["schema"]["root"]) - set(source_schema))
        if len(new_columns) == 0:
            return None

        logger.info(f'Adding new columns : {",".join(new_columns)}')
        for col in new_columns:
            if "NOT NULL" in col:
                col = col.replace("NOT NULL", "")
            add_column_query = (
                f"ALTER TABLE {self.meta_data['database']['table']} ADD COLUMN({col})"
            )
            self.meta_data["sql"][col.split(" ")[0]] = add_column_query
            logger.info(
                f"---Added column {col} to table {self.meta_data['database']['table']}"
            )

    @with_logging(logger_module=logger)
    def prepare_upsert_sql(self):
        target_schema_str = (
            ", ".join(
                [
                    f"{field.field_name} {field.field_type}"
                    for field in self.contract.contract_fields
                ]
            )
            + ", InsertedTimestamp Timestamp, UpdatedTimestamp Timestamp"
        )
        final_cols = [field.field_name for field in self.contract.contract_fields] + [
            "InsertedTimestamp",
            "UpdatedTimestamp",
        ]
        processed_path = self.meta_data["path"]["processed_full_path"]
        # add Landing date if it is Bronze Processed layer
        if (
            self.meta_data["layer"] == "Bronze"
            and self.meta_data["stage"] == "Processed"
        ):
            target_schema_str = (
                target_schema_str + ", YYYY String, MM String, DD String"
            )
            final_cols = [
                field.field_name for field in self.contract.contract_fields
            ] + ["InsertedTimestamp", "UpdatedTimestamp", "YYYY", "MM", "DD"]

        update_exclude_cols = list(self.meta_data["schema"]["primary_keys"])
        update_exclude_cols.append("InsertedTimestamp")
        update_cols = list(filter(lambda v: v not in update_exclude_cols, final_cols))

        self.meta_data["sql"] = {
            "creation_schema": f"""CREATE SCHEMA IF NOT EXISTS {self.meta_data['database']['schema']} LOCATION '{self.meta_data['database']['schema_location']}'""",
            "use_schema": f"use {self.meta_data['database']['schema']}",
            "creation_table": f"""CREATE TABLE IF NOT EXISTS {self.meta_data['database']['table']} ({target_schema_str}) USING DELTA PARTITIONED BY ({self.meta_data['database'].get('partition_by','YYYY,MM,DD')}) LOCATION "{processed_path}" """,
        }
        # logger.info(self.meta_data["schema"]["primary_keys"])
        # is_delta_table = DeltaTable.isDeltaTable(self.spark, processed_path)
        # if is_delta_table:
        #     source_schema_list = self.get_ddl_from_input_data(processed_path).split(",")
        #     self.check_addition_columns(source_schema=source_schema_list)
        self.meta_data["schema"] = {
            "join_string": " AND ".join(
                [
                    f'COALESCE(Source.{col_name}, "NotAvailable") = COALESCE(Target.{col_name}, "NotAvailable")'
                    for col_name in self.meta_data["schema"]["primary_keys"]
                ]
            ),
            "update_string": ", ".join(
                [f"Target.{col_name} = Source.{col_name}" for col_name in update_cols]
            ),
            "source_insert_string": ", ".join(
                [f"Source.{col_name}" for col_name in final_cols]
            ),
            "target_insert_string": ", ".join(
                [f"Target.{col_name}" for col_name in final_cols]
            ),
        }
        self.meta_data["sql"][
            "upsert_string"
        ] = f"""
            MERGE INTO {self.meta_data["database"]["table"]} AS Target
            USING {self.meta_data["database"]["source_table"]} AS Source
            ON {self.meta_data["schema"]["join_string"]}
            WHEN MATCHED THEN UPDATE SET {self.meta_data["schema"]["update_string"]}
            WHEN NOT MATCHED THEN INSERT ({self.meta_data["schema"]["target_insert_string"]}) VALUES ({self.meta_data["schema"]["source_insert_string"]})
        """

    @with_logging(logger_module=logger)
    def prepare_scd_type_2_sql(self):
        target_schema_str = (
            ", ".join(
                [
                    f"{field.field_name} {field.field_type}"
                    for field in self.contract.contract_fields
                ]
            )
            + ", InsertedTimestamp Timestamp, UpdatedTimestamp Timestamp, ValidFrom Date, ValidTo Date, IsValid Boolean"
        )

        update_fields = list(
            filter(lambda x: x.field_is_primary is False, self.contract.contract_fields)
        )
        update_cols = [field.field_name for field in update_fields]

        self.meta_data["sql"] = {
            "creation_schema": f"""
                                CREATE SCHEMA IF NOT EXISTS {self.meta_data['database']['schema']}
                                LOCATION '{self.meta_data['database']['schema_location']}'
                                """,
            "use_schema": f"use {self.meta_data['database']['schema']}",
            "creation_table": f"""
                                CREATE TABLE IF NOT EXISTS {self.meta_data['database']['table']} ({target_schema_str})
                                USING DELTA PARTITIONED BY ({self.meta_data['database'].get('partition_by','IsValid')})
                                LOCATION "{self.meta_data['path']['processed_full_path']}"
                                """,
        }
        self.meta_data["schema"] = {
            "join_string": " AND ".join(
                [
                    f'COALESCE(Source.{col_name}, "NotAvailable") = COALESCE(Target.{col_name}, "NotAvailable")'
                    for col_name in self.meta_data["schema"]["primary_keys"]
                ]
                + ["Target.IsValid"]
            ),
            "primary_key_is_null": " is null and ".join(
                [
                    f"Target.{col_name}"
                    for col_name in self.meta_data["schema"]["primary_keys"]
                ]
            )
            + " is null",
            "column_changed_condition": " OR ".join(
                [
                    f'COALESCE(Target.{col_name}, "") <> COALESCE(Source.{col_name}, "")'
                    for col_name in update_cols
                ]
            ),
            "primary_keys_alias": ", ".join(
                [
                    f"Target.{col_name} AS {col_name}_alias"
                    for col_name in self.meta_data["schema"]["primary_keys"]
                ]
            ),
            "update_existing_condition": " AND ".join(
                [
                    f"{col_name}={col_name}_alias"
                    for col_name in self.meta_data["schema"]["primary_keys"]
                ]
            ),
            "primary_key_list": ", ".join(
                [col_name for col_name in self.meta_data["schema"]["primary_keys"]]
            ),
            "primary_alias_key_list": ", ".join(
                [
                    f"{col_name}_alias"
                    for col_name in self.meta_data["schema"]["primary_keys"]
                ]
            ),
        }
        self.meta_data["sql"][
            "insert_new_records_sql"
        ] = f"""
        INSERT INTO {self.meta_data['database']['table']}
        (
            SELECT Source.*, '{self.landing_date}' as ValidFrom, null as ValidTo, true as IsValid
            FROM {self.meta_data['database']['table']} Target
            RIGHT JOIN  {self.meta_data["database"]["source_table"]} Source
            ON {self.meta_data["schema"]["join_string"]}
            WHERE {self.meta_data["schema"]["primary_key_is_null"]}
        );
        """
        self.meta_data["sql"][
            "create_stg_update_table_sql"
        ] = f"""
        CREATE TABLE {self.meta_data['database']['table']}_Stg AS
        (
            SELECT {self.meta_data['schema']['primary_keys_alias']}
            FROM {self.meta_data['database']['table']} Target
            JOIN {self.meta_data["database"]["source_table"]} Source
            ON {self.meta_data['schema']['join_string']}
            WHERE {self.meta_data['schema']['column_changed_condition']}
        );
        """
        self.meta_data["sql"][
            "update_existing_sql"
        ] = f"""
        MERGE INTO {self.meta_data['database']['table']}
        USING {self.meta_data['database']['table']}_Stg ON {self.meta_data['schema']['update_existing_condition']} and IsValid=true
        WHEN MATCHED THEN UPDATE SET IsValid=false, ValidTo=DATE('{self.landing_date}'), UpdatedTimestamp=TIMESTAMP('{self.current_timestamp}');
        """
        self.meta_data["sql"][
            "insert_new_change_sql"
        ] = f"""
        INSERT INTO {self.meta_data['database']['table']}
        (
            SELECT *, '{self.landing_date}' as ValidFrom, null as ValidTo, true as IsValid
            FROM {self.meta_data["database"]["source_table"]}
            WHERE ({self.meta_data['schema']['primary_key_list']}) IN
            (
                SELECT DISTINCT {self.meta_data['schema']['primary_alias_key_list']}
                FROM {self.meta_data['database']['table']}_Stg
            )
        );
        """
        self.meta_data["sql"][
            "drop_stg_table_sql"
        ] = f"""
        DROP TABLE {self.meta_data['database']['table']}_Stg
        """

    @with_logging(logger_module=logger)
    def prepare_snapshot_sql(self):
        partition_columns = self.meta_data["database"].get("partition_by").split(",")
        columns_list = list(
            filter(
                lambda x: x.field_name.lower() not in partition_columns,
                self.contract.contract_fields,
            )
        )
        target_schema_str = (
            ", ".join(
                [
                    f"{field.field_name} {field.field_type}"
                    for field in self.contract.contract_fields
                ]
            )
            + ", InsertedTimestamp Timestamp, UpdatedTimestamp Timestamp"
        )

        select_first_partition_str = ", ".join(
            [f"first({col}) as {col}" for col in partition_columns]
        )
        select_first_partition_sql = f"""
                                    select {select_first_partition_str} from {self.meta_data["database"]["source_table"]}
                                    """
        first_partition_df = self.spark.sql(select_first_partition_sql).first()

        self.meta_data["schema"] = {
            "columns_list": ", ".join([field.field_name for field in columns_list])
            + ", InsertedTimestamp, UpdatedTimestamp",
            "partition_condition": ", ".join(
                [
                    f"{col} = '{first_partition_df[f'{col}']}'"
                    for col in partition_columns
                ]
            ),
        }

        self.meta_data["sql"] = {
            "creation_schema": f"""
                                CREATE SCHEMA IF NOT EXISTS {self.meta_data['database']['schema']}
                                LOCATION '{self.meta_data['database']['schema_location']}'
                                """,
            "use_schema": f"use {self.meta_data['database']['schema']}",
            "creation_table": f"""
                                CREATE TABLE IF NOT EXISTS {self.meta_data['database']['table']} ({target_schema_str})
                                USING DELTA PARTITIONED BY ({self.meta_data['database'].get('partition_by')})
                                LOCATION "{self.meta_data['path']['processed_full_path']}"
                                """,
            "snapshot": f"""
                        INSERT OVERWRITE {self.meta_data["database"]["table"]}
                        PARTITION ({self.meta_data["schema"]["partition_condition"]})
                        SELECT {self.meta_data["schema"]["columns_list"]} FROM {self.meta_data["database"]["source_table"]}
                        """,
        }

    @with_logging(logger_module=logger)
    def prepare_overwrite_sql(self):
        partition_by = self.meta_data["database"].get("partition_by")
        overwrite_by = self.meta_data["database"].get("overwrite_cols", None)
        partition_columns = partition_by.split(",") if partition_by != None else []
        overwrite_cols = overwrite_by.split(",") if overwrite_by != None else []

        target_schema_str = (
            ", ".join(
                [
                    f"{field.field_name} {field.field_type}"
                    for field in self.contract.contract_fields
                ]
            )
            + ", InsertedTimestamp Timestamp, UpdatedTimestamp Timestamp"
        )
        final_cols = [field.field_name for field in self.contract.contract_fields] + [
            "InsertedTimestamp",
            "UpdatedTimestamp",
        ]
        if (
            self.meta_data["layer"] == "Bronze"
            and self.meta_data["stage"] == "Processed"
        ):
            target_schema_str = (
                target_schema_str + ", YYYY String, MM String, DD String"
            )
            final_cols = final_cols + ["YYYY", "MM", "DD"]

        self.meta_data["sql"] = {
            "use_schema": f"use {self.meta_data['database']['schema']}",
            "creation_table": f"""
                                CREATE TABLE IF NOT EXISTS {self.meta_data['database']['table']} ({target_schema_str})
                                USING DELTA PARTITIONED BY ({self.meta_data['database'].get('partition_by')})
                                LOCATION "{self.meta_data['path']['processed_full_path']}"
                                """,
        }

        if set(partition_columns) == set(overwrite_cols) or not overwrite_cols:
            logger.info(
                f"Start preparing SQL to insert overwrite on partition {partition_columns}"
            )

            self.meta_data["schema"] = {"columns_list": ", ".join(final_cols)}
            self.meta_data["sql"][
                "overwrite"
            ] = f"""
                            INSERT OVERWRITE {self.meta_data["database"]["table"]}
                            SELECT {self.meta_data["schema"]["columns_list"]} FROM {self.meta_data["database"]["source_table"]}
                            """
        else:
            logger.info(f"Start preparing overwrite SQL based on keys {overwrite_cols}")
            self.meta_data["schema"] = {
                "join_string": " AND ".join(
                    [
                        f"Source.{col_name} = Target.{col_name}"
                        for col_name in overwrite_cols
                    ]
                ),
                "source_insert_string": ", ".join(
                    [f"Source.{col_name}" for col_name in final_cols]
                ),
                "target_insert_string": ", ".join(
                    [f"{col_name}" for col_name in final_cols]
                ),
            }

            self.meta_data["sql"][
                "delete_matched"
            ] = f"""
                            MERGE INTO {self.meta_data["database"]["table"]} AS Target USING {self.meta_data["database"]["source_table"]} AS Source
                            ON {self.meta_data['schema']['join_string']}
                            WHEN MATCHED THEN DELETE
                            """

            self.meta_data["sql"][
                "insert_not_matched"
            ] = f"""
                            INSERT INTO {self.meta_data["database"]["table"]} ({self.meta_data["schema"]["target_insert_string"]})
                            SELECT {self.meta_data["schema"]["source_insert_string"]} FROM {self.meta_data["database"]["source_table"]} AS Source
                            """

    @with_logging(logger_module=logger)
    def is_empty_dataframe(self, dataframe: DataFrame):
        if len(dataframe.take(1)) == 0:
            raise ExtractEmptyFileWithNoRecord(
                f'File {self.meta_data["file_ingest"]} return 0 records. Abort '
            )

    @with_logging(logger_module=logger)
    def get_extracted(self):
        if self.cache_level == StockETLStoreLevel.ADLS:
            return self.spark.read.format("parquet").load(
                self.meta_data["processing"]["extracted_path"]
            )
        else:
            return self.extracted_df

    @with_logging(logger_module=logger)
    def get_transformed(self):
        if self.cache_level == StockETLStoreLevel.ADLS:
            return self.spark.read.format("parquet").load(
                self.meta_data["processing"]["transformed_path"]
            )
        else:
            return self.transformed_df

    @with_logging(logger_module=logger)
    def get_valid(self):
        if self.cache_level == StockETLStoreLevel.ADLS:
            return self.spark.read.format("parquet").load(
                self.meta_data["processing"]["valid_path"]
            )
        else:
            return self.valid_df

    @with_logging(logger_module=logger)
    def get_invalid(self):
        if self.cache_level == StockETLStoreLevel.ADLS:
            return self.spark.read.format("parquet").load(
                self.meta_data["processing"]["invalid_path"]
            )
        else:
            return self.invalid_df

    @with_logging(logger_module=logger)
    def persist_extracted(self, dataframe: DataFrame):
        if self.cache_level == StockETLStoreLevel.ADLS:
            dataframe.write.format("parquet").mode("overwrite").save(
                self.meta_data["processing"]["extracted_path"]
            )
        else:
            self.extracted_df = dataframe.persist(
                getattr(StockETLStoreLevel, self.cache_level)
            )
        return self.get_extracted()

    @with_logging(logger_module=logger)
    def persist_transformed(self, dataframe: DataFrame):
        if self.cache_level == StockETLStoreLevel.ADLS:
            dataframe.write.format("parquet").mode("overwrite").save(
                self.meta_data["processing"]["transformed_path"]
            )
        else:
            self.transformed_df = dataframe.persist(
                getattr(StockETLStoreLevel, self.cache_level)
            )
        return self.get_transformed()

    @with_logging(logger_module=logger)
    def persist_valid(self, dataframe: DataFrame):
        if self.cache_level == StockETLStoreLevel.ADLS:
            dataframe.write.format("parquet").mode("overwrite").save(
                self.meta_data["processing"]["valid_path"]
            )
        else:
            self.valid_df = dataframe.persist(
                getattr(StockETLStoreLevel, self.cache_level)
            )
        return self.get_valid()

    @with_logging(logger_module=logger)
    def persist_invalid(self, dataframe: DataFrame):
        if self.cache_level == StockETLStoreLevel.ADLS:
            dataframe.write.format("parquet").mode("overwrite").save(
                self.meta_data["processing"]["invalid_path"]
            )
        else:
            self.invalid_df = dataframe.persist(
                getattr(StockETLStoreLevel, self.cache_level)
            )
        return self.get_invalid()
