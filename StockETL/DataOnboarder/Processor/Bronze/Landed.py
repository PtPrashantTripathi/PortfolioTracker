from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from StockETL.Common.CustomLogger import (
    get_logger,
    with_logging,
    with_processor_logging,
)
from StockETL.DataOnboarder.Processor.BaseProcessor import BaseProcessor

logger = get_logger(name="StockETL - BRONZE - LANDED - PROCESSOR")


@with_logging(logger_module=logger)
class BronzeLandedProcessor(BaseProcessor):
    """Bronze Landed Processor"""

    def __extract_file(self, file_path, contract_cols):
        extracted_df = self.reader.ingest_file(file_path)
        extracted_cols = extracted_df.columns
        missing_cols = []
        if "InputFileName" not in extracted_cols:
            extracted_df = extracted_df.withColumn("InputFileName", F.input_file_name())
        for col in contract_cols:
            if col not in extracted_cols:
                missing_cols.append(col)
        if len(missing_cols) > 0:
            # try to get dataAddress options
            dataAddress = self.reader.override_options.get("dataAddress", None)
            raise Exception(
                f"Missing {missing_cols} columns in source file{f' at {dataAddress}' if dataAddress != None else ''}. Actual source columns: {extracted_cols}"
            )
        filter_not_blank_row_condition = (
            " is not null or ".join([f"`{col}`" for col in contract_cols])
            + " is not null"
        )  # select at least 1 column has data
        return extracted_df.filter(filter_not_blank_row_condition)

    @with_processor_logging(logger_module=logger)
    def extract(self) -> DataFrame:
        """Extract Bronze Landed"""
        input_df = None
        contract_cols = self.contract.generate_source_column_list()
        self.extract_status_list = []
        if isinstance(self.input_data, list) and self.sequential_extraction:
            file_path_list = self.input_data
            for file_path in file_path_list:
                file_name = file_path.split("/").pop()
                status = "SUCCESS"
                try:
                    extracted_df = self.__extract_file(file_path, contract_cols).select(
                        *contract_cols, "InputFileName"
                    )
                    if input_df == None:
                        input_df = extracted_df
                    else:
                        input_df = input_df.union(extracted_df)
                except Exception as e:
                    status = f"FAILED: {str(e)}"
                self.extract_status_list.append([file_name, status])
        else:
            input_df = self.__extract_file(self.input_data, contract_cols).select(
                *contract_cols, "InputFileName"
            )

        input_df = self.custom_udf_func(input_df, "EXTRACT")
        return self.persist_extracted(input_df)

    def validate(self):
        """validate for Bronze Landed"""
        validator = self.get_validator(self.get_extracted())
        self.extract_result_df = validator.extract_validation(self.extract_status_list)
        self.is_empty_dataframe(self.get_extracted())
        error_df = validator.skip_validation()
        return self.persist_invalid(error_df)

    @with_processor_logging(logger_module=logger)
    def transform(
        self,
    ) -> DataFrame:
        """Transform Bronze Landed"""
        # Adding Audit Trail
        ## add alias for input file name column to alias expression schema
        alias_expr = self.meta_data["schema"]["alias_expr"]
        alias_expr.append("`InputFileName` AS `inputfilename`")
        transform_df = (
            self.get_extracted()
            .selectExpr(alias_expr)
            .withColumn("YYYY", F.lit(self.meta_data["landing"]["year"]))
            .withColumn("MM", F.lit(self.meta_data["landing"]["month"]))
            .withColumn("DD", F.lit(self.meta_data["landing"]["day"]))
        )
        transform_df = self.custom_udf_func(transform_df, "TRANSFORM")
        void_columns = [
            c
            for c in transform_df.columns
            if transform_df.select(c).dtypes[0][1] == "void"
        ]
        for c in void_columns:
            transform_df = transform_df.withColumn(c, F.col(c).cast("string"))
        return self.persist_transformed(transform_df)

    @with_processor_logging(logger_module=logger)
    def load(self):
        """Load Bronze Landed"""
        output_df = self.custom_udf_func(self.get_transformed(), "LOAD")
        output_df.write.format(self.meta_data["object_type"]).mode(
            self.meta_data["save_mode"]
        ).save(self.meta_data["path"]["full_path"])
        return output_df

    def __repr__(self) -> str:
        return "Processor - Bronze - Landed"
