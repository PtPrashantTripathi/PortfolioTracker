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

logger = get_logger(name="StockETL - BRONZE - PROCESSED - PROCESSOR")


@with_logging(logger_module=logger)
class BronzeProcessedProcessor(BaseProcessor):
    """Bronze Processed Processor"""

    @with_processor_logging(logger_module=logger)
    def extract(self) -> DataFrame:
        """Extract Bronze Processed"""
        input_df = self.reader.ingest_file()
        self.is_empty_dataframe(input_df)
        input_df = self.custom_udf_func(input_df, "EXTRACT")
        return self.persist_extracted(input_df)

    @with_processor_logging(logger_module=logger)
    def validate(self):
        """Validate Bronze Processed"""
        input_df = self.build_generated_primary_keys(self.get_extracted())
        validator = self.get_validator(input_df)
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
        return self.persist_invalid(invalid_df)

    @with_processor_logging(logger_module=logger)
    def transform(self) -> DataFrame:
        """Transform Bronze Processed"""
        transform_df = self.drop_generated_primary_keys(self.get_valid())
        transform_df = (
            transform_df.selectExpr(self.meta_data["schema"]["cast_expr"])
            .withColumn("InsertedTimestamp", F.current_timestamp())
            .withColumn("UpdatedTimestamp", F.current_timestamp())
            .withColumn("YYYY", F.lit(self.meta_data["landing"]["year"]))
            .withColumn("MM", F.lit(self.meta_data["landing"]["month"]))
            .withColumn("DD", F.lit(self.meta_data["landing"]["day"]))
        )

        transform_df = self.custom_udf_func(transform_df, "TRANSFORM")
        return self.persist_transformed(transform_df)

    @with_processor_logging(logger_module=logger)
    def load(self) -> None:
        """Load Bronze Processed"""
        output_df = self.get_transformed()
        self.prepare_load_sql(self.meta_data["save_mode"])
        self.spark.catalog.dropTempView(self.meta_data["database"]["source_table"])  # type: ignore
        output_df.createOrReplaceTempView(self.meta_data["database"]["source_table"])
        for key in self.meta_data["sql"]:
            sql_string = self.meta_data["sql"][key]
            logger.info(f"Commit SQL {key}: `{sql_string}`")
            self.commit_sql(sql_string=sql_string)
        success_message = f"UPSERT COMPLETE to {self.meta_data['database']['schema']}"
        logger.info(success_message)
        return output_df

    def __repr__(self) -> str:
        return "Processor - Bronze - Processed"
