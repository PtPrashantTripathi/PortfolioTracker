from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from StockETL.core_error import ValidationError
from StockETL.Common.Constants import ERROR_LEVEL
from StockETL.Common.CustomLogger import (
    get_logger,
    with_logging,
    with_processor_logging,
)
from StockETL.DataOnboarder.Processor.BaseProcessor import BaseProcessor

logger = get_logger(name="StockETL - SILVER - DIMENSION - PROCESSOR")


@with_logging(logger_module=logger)
class SilverDimensionProcessor(BaseProcessor):
    """Silver Dimension Processor"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    @with_processor_logging(logger_module=logger)
    def extract(self) -> DataFrame:
        """Extract Silver Dimension"""
        input_df = self.reader.ingest_file()
        input_df = self.custom_udf_func(input_df, "EXTRACT")
        input_df = input_df.selectExpr(self.meta_data["schema"]["alias_expr"])
        return self.persist_extracted(input_df)

    @with_processor_logging(logger_module=logger)
    def validate(self):
        """Validate Silver Dimension"""
        input_df = self.build_generated_primary_keys(self.get_extracted())
        validator = self.get_validator(input_df)
        if not self.meta_data["skip_validation"]:
            (
                valid_df,
                invalid_df,
                summarize_error_df,
            ) = validator.execute_validation()
            self.persist_valid(valid_df)
            if len(summarize_error_df.take(1)) == 1:
                summarize_error_df.write.mode("append").saveAsTable(
                    "default.summarize_error"
                )
                critical_error_list = summarize_error_df.filter(
                    F.col("udf_max_error_level").contains(ERROR_LEVEL.CRITICAL.name)
                ).collect()
                if critical_error_list:
                    raise ValidationError(critical_error_list)
        else:
            self.persist_valid(input_df)
            invalid_df = validator.skip_validation()
        return self.persist_invalid(invalid_df)

    @with_processor_logging(logger_module=logger)
    def transform(self) -> DataFrame:
        """Transform Silver Dimension"""
        # field_name_list = [field.field_name for field in self.contract.contract_fields]
        valid_df = self.drop_generated_primary_keys(self.get_valid())
        transform_df = (
            valid_df
            .withColumn("InsertedTimestamp", F.lit(self.current_timestamp))
            .withColumn("UpdatedTimestamp", F.lit(self.current_timestamp))
        )
        transform_df = self.custom_udf_func(transform_df, "TRANSFORM")
        return self.persist_transformed(transform_df)

    @with_logging(logger_module=logger)
    def load(self) -> None:
        """Load Silver Dimension"""
        self.spark.catalog.dropTempView(self.meta_data["database"]["source_table"])  # type: ignore
        self.get_transformed().createOrReplaceTempView(
            f'{self.meta_data["database"]["source_table"]}'
        )
        self.prepare_load_sql(self.meta_data["save_mode"])
        for key in self.meta_data["sql"]:
            sql_string = self.meta_data["sql"][key]
            message = f"Commit SQL {key}: `{sql_string}`"
            logger.info(message)
            self.commit_sql(sql_string=sql_string)
        success_message = (
            f"{self.meta_data['save_mode']} to {self.meta_data['database']['schema']}"
        )
        logger.info(success_message)

    def __repr__(self) -> str:
        return f"Processor - Silver - Dimension"
