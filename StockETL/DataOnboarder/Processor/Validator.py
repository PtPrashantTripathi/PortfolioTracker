from typing import Any, Union

from pyspark.sql import Window, DataFrame, SparkSession, types
from pyspark.sql import functions as F

from StockETL.Common import CustomSpark
from StockETL.Common.Constants import ERROR_LEVEL
from StockETL.Common.CustomLogger import (
    get_logger,
    with_logging,
    with_processor_logging,
)
from StockETL.DataContract.Contract import Contract

spark = CustomSpark.get_spark_session()

logger = get_logger("StockETL - VALIDATOR")


@with_logging(logger_module=logger)
class UValidator:
    """Base class validator"""

    def __init__(self, input_df: Any, contract: Contract, meta_data: dict) -> Any:
        self.input_df = input_df
        self.contract = contract
        self.meta_data = meta_data
        self.primary_keys = (
            "RECID"
            if len(self.meta_data["schema"]["primary_keys"]) > 1
            else self.meta_data["schema"]["primary_keys"]
        )

    @with_logging(logger_module=logger)
    def skip_validation(self):
        """Return a empty DataFrame"""
        return (
            SparkSession.getActiveSession()
            .createDataFrame([], self.input_df.schema)
            .withColumn("error_level", F.array().cast("array<string>"))
            .withColumn("row_status_reason", F.array().cast("array<string>"))
            .withColumn("udf_max_error_level", F.lit(None).cast("string"))
            .withColumn("job_id", F.lit(self.meta_data["job_id"]))
        )

    def extract_validation(self, extract_status_list):
        """Return a DataFrame contains a list of invalid files and status"""
        status_file_schema = types.StructType(
            [
                types.StructField("file_name", types.StringType(), True),
                types.StructField("row_status_reason", types.StringType(), True),
            ]
        )
        return (
            SparkSession.getActiveSession()
            .createDataFrame(extract_status_list, status_file_schema)
            .withColumn("job_id", F.lit(self.meta_data["job_id"]))
        )

    @with_logging(logger_module=logger)
    def __pre_validation(self):
        """Pre validation for adding neccessary fields"""
        # add temp index to transform data
        self.input_df = (
            self.input_df.withColumn(
                "temp_idx",
                F.monotonically_increasing_id(),
            )
            .withColumn("row_status_reason", F.lit(""))
            .withColumn("error_level", F.lit(""))
        )
        self.error_df = spark.createDataFrame([], self.input_df.schema)

    @with_logging(logger_module=logger)
    def duplication(self) -> Union[DataFrame, DataFrame]:
        """Duplication"""

        window_partition_by_primary_key = Window.partitionBy(self.primary_keys).orderBy(
            F.lit(1)
        )
        self.input_df = self.input_df.withColumn(
            "row_status_reason",
            F.row_number().over(window_partition_by_primary_key),
        ).withColumn(
            "keep_row_idx",
            (
                F.min("row_status_reason").over(window_partition_by_primary_key)
                if self.meta_data["duplicated_keep_option"] == "first"
                else F.max("row_status_reason").over(window_partition_by_primary_key)
            ),
        )
        self.error_df = self.input_df.filter(
            (F.col("row_status_reason") != F.col("keep_row_idx")).alias("duplication")
        )
        self.error_df = (
            self.error_df.withColumn(
                "row_status_reason", F.lit("Duplication within batch ingestions")
            )
            .withColumn("error_level", F.lit(ERROR_LEVEL.ERROR.name))
            .drop("keep_row_idx")
        )
        # Clean up Duplication from transform df
        self.input_df = self.input_df.filter(
            F.col("row_status_reason") == F.col("keep_row_idx")
        ).drop("keep_row_idx")

    @with_logging(logger_module=logger)
    def validation_data_type(self, field):
        """Validate field data type"""
        try_cast_str = self.contract.get_cast_expr(field, is_try=True)
        # transform try_cast
        self.input_df = self.input_df.withColumn(
            f"try_{field.field_name}", F.expr(try_cast_str)
        )
        invalid_data_type_df = self.input_df.filter(
            F.col(field.field_name).isNotNull()
            & F.col(f"try_{field.field_name}").isNull()
        )
        invalid_data_type_df = (
            invalid_data_type_df.withColumn(
                "row_status_reason",
                F.format_string(
                    "The value '%s' in column `%s` could not be casted to %s type.",
                    F.col(field.field_name),
                    F.lit(field.field_name),
                    F.lit(field.field_type),
                ),
            ).withColumn("error_level", F.lit(ERROR_LEVEL.ERROR.name))
        ).drop(f"try_{field.field_name}")
        self.error_df = self.error_df.unionByName(invalid_data_type_df)
        self.input_df = self.input_df.drop(f"try_{field.field_name}")

    @with_logging(logger_module=logger)
    def validation_not_nullable(self, col_name, conf, level):
        is_nullable = conf.get("value", None)
        if not is_nullable:
            is_null_df = (
                self.input_df.filter(F.col(col_name).isNull())
                .withColumn("error_level", F.lit(level))
                .withColumn(
                    "row_status_reason", F.lit(f"Column '{col_name}' must not be null.")
                )
            )
            self.error_df = self.error_df.unionByName(is_null_df)

    @with_logging(logger_module=logger)
    def validation_string_pattern(self, col_name, conf, level):
        regex_pattern = conf.get("value", None)
        if not regex_pattern:  # skip validate
            return self.input_df, self.error_df
        invalid_string_pattern_df = (
            self.input_df.filter(~F.col(col_name).rlike(regex_pattern))
            .withColumn("error_level", F.lit(level))
            .withColumn(
                "row_status_reason",
                F.format_string(
                    f"Column '{col_name}' with value '%s' does not match the expected format.",
                    F.col(col_name),
                ),
            )
        )
        self.error_df = self.error_df.unionByName(invalid_string_pattern_df)

    def __get_numeric_operator(selft, operator_name):
        operator_dict = {
            "eq": "==",
            "ne": "<>",
            "gt": ">",
            "ge": ">=",
            "lt": "<",
            "le": "<=",
        }
        return operator_dict.get(operator_name, None)

    @with_logging(logger_module=logger)
    def validation_numeric_constraint(self, col_name, conf, level):
        operator = conf.get("operator", None)
        value = conf.get("value", None)
        if operator is None or self.__get_numeric_operator(operator):
            raise Exception(
                f"Numeric Contraints Validation: Doesn't support operator '{operator}'"
            )
        if value is None:
            raise Exception(
                f"Numeric Constraints Validation: Compared value must not be null - Received {value}"
            )

        invalid_numeric_contraint_df = (
            self.input_df.filter(
                f"{col_name} is null or not {col_name} {self.__get_numeric_operator(operator)} {value}"
            )
            .withColumn("error_level", F.lit(level))
            .withColumn(
                "row_status_reason",
                F.format_string(
                    f"Column '{col_name}' with value '%s' does not match expected value: {self.__get_numeric_operator(operator)} {value}.",
                    F.col(col_name),
                ),
            )
        )
        self.error_df = self.error_df.unionByName(invalid_numeric_contraint_df)

    @with_logging(logger_module=logger)
    def primary_key_not_null(self) -> Union[DataFrame, DataFrame]:
        """Not Null Pirmary Keys"""
        for col in self.meta_data["schema"]["primary_keys"]:
            temp_df = (
                self.input_df.filter(F.col(col).isNull())
                .withColumn(
                    "row_status_reason", F.lit(f"ValueError in Primary keys - {col}")
                )
                .alias("temp")
            )
            self.error_df = self.error_df.union(temp_df)
            self.input_df = self.input_df.filter(F.col(col).isNotNull())
        # TODO: Other Validation from contract

    def execute_single_logic(self, field, logic, logic_type: str):
        if logic_type == "regex":
            regex_function = F.regexp_replace(
                field.field_name, logic["computed"]["value"], ""
            )
            temp_df = self.input_df.withColumn(field.field_name, regex_function)
            self.error_df = self.error_df.unionByName(temp_df, allowMissingColumns=True)
            cond = " AND ".join(
                [
                    f"input_df.{x}=temp_df.{x}"
                    for x in self.contract.generate_primary_keys()
                ]
            )
            self.input_df = self.input_df.alias("input_df").join(
                temp_df.alias("temp_df"), cond, how="left_outer"
            )
        if logic_type == "range":
            logger.info("TODO: implement Range number")

    @with_logging(logger_module=logger)
    def logic_parse_contract(self) -> Union[DataFrame, DataFrame]:
        """Logic within batch or generated from contract"""
        for field in self.contract.contract_fields:
            for logic in field.field_validations:
                if logic.get("computed", None):
                    self.execute_single_logic(field, logic, "regex")
                    self.execute_single_logic(field, logic, "range")

    @with_logging(logger_module=logger)
    def __post_validation(self):
        """Post validation for generating report"""

        def ___max_error_level(list_of_values):
            enum_val_list = [ERROR_LEVEL[x] for x in list_of_values]
            return max(enum_val_list).name

        udf_max_error_level = F.udf(___max_error_level, types.StringType())
        # drop report fields
        self.input_df = self.input_df.drop("row_status_reason", "error_level")
        if self.error_df.count() > 0:
            self.invalid_df = (
                self.error_df.groupby(self.input_df.columns)
                .agg(
                    F.collect_list("error_level").alias("error_level"),
                    F.collect_list("row_status_reason").alias("row_status_reason"),
                )
                .withColumn(
                    "udf_max_error_level", udf_max_error_level(F.col("error_level"))
                )
                .withColumn("job_id", F.lit(self.meta_data["job_id"]))
            )
            # filter valid report to transform data
            self.input_df = self.input_df.join(
                self.invalid_df.filter(
                    F.col("udf_max_error_level") != ERROR_LEVEL.WARN.name
                ),
                on="temp_idx",
                how="left_anti",
            )
            self.summarize_error_df = (
                (
                    self.error_df.withColumn(
                        "keys",
                        F.to_json(
                            F.struct(self.primary_keys),
                            options={"ignoreNullFields": False},
                        ),
                    )
                    .groupBy(["temp_idx"])
                    .agg(
                        F.collect_list("error_level").alias("error_level"),
                        F.collect_set("row_status_reason").alias("row_status_reason"),
                        F.collect_set("keys").alias("keys"),
                    )
                    .withColumn(
                        "udf_max_error_level", udf_max_error_level(F.col("error_level"))
                    )
                    .withColumn("job_id", F.lit(self.meta_data["job_id"]))
                )
                .drop(*self.meta_data["schema"]["primary_keys"])
                .drop("temp_idx")
            )
        else:
            self.invalid_df = (
                SparkSession.getActiveSession()
                .createDataFrame([], self.input_df.schema)
                .withColumn("error_level", F.array().cast("array<string>"))
                .withColumn("row_status_reason", F.array().cast("array<string>"))
                .withColumn("udf_max_error_level", F.lit(None).cast("string"))
                .withColumn("job_id", F.lit(self.meta_data["job_id"]))
            )
            self.summarize_error_df = (
                SparkSession.getActiveSession()
                .createDataFrame([], self.input_df.schema)
                .withColumn("error_level", F.array().cast("array<string>"))
                .withColumn("row_status_reason", F.array().cast("array<string>"))
                .withColumn("udf_max_error_level", F.lit(None).cast("string"))
                .withColumn("job_id", F.lit(self.meta_data["job_id"]))
                .drop(*self.meta_data["schema"]["primary_keys"])
                .drop("temp_idx")
            )
        self.input_df = self.input_df.drop("temp_idx")
        # summarize error to report

    def __get_validator_func(self, validator_name):
        validator_dict = {
            "is_nullable": self.validation_not_nullable,
            "string_format": self.validation_string_pattern,
            "numeric_constraint": self.validation_numeric_constraint,
        }
        return validator_dict.get(validator_name, None)

    @with_logging(logger_module=logger)
    def execute_validation(self):
        self.__pre_validation()
        self.duplication()
        for field in self.contract.contract_fields:
            self.validation_data_type(field)
            for rule in field.field_validations:
                rule_name, conf = next(iter(rule.items()))
                validate_func = self.__get_validator_func(rule_name)
                if validate_func != None:
                    validate_func(
                        field.field_name,
                        conf,
                        conf.get("level", ERROR_LEVEL.WARN.name),
                    )
        logger.info(f"pre-post_validation: {self.input_df.count():,.0f}")
        self.__post_validation()
        logger.info(f"after-post_validation: {self.input_df.count():,.0f}")
        return self.input_df, self.invalid_df, self.summarize_error_df
