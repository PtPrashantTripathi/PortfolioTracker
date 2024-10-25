import os
import re
from datetime import datetime

from pyspark.sql.types import (
    LongType,
    FloatType,
    DoubleType,
    StringType,
    StructType,
    BooleanType,
    IntegerType,
    StructField,
    TimestampType,
)

__doc__ = """Helper common functions, all in one file"""


class ContractFunction:
    @staticmethod
    def validate_email(email: str):
        """Validate email format from string"""
        EMAIL_REGEX = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
        if re.fullmatch(EMAIL_REGEX, email):
            return email
        raise ValueError(f"Not an email: {email}")


class StockETLFunction:
    @staticmethod
    def is_local() -> bool:
        """Checking if local dev is enable

        :return: True or False depend on local dev
        :rtype: bool
        """
        return bool(os.environ.get("LOCAL_DEV", False))

    @staticmethod
    def get_processor(layer="Bronze", stage="Landed"):
        factory = {
            "Bronze": {
                "Landed": "BronzeLandedProcessor",
                "Processed": "BronzeProcessedProcessor",
            },
            "Silver": {
                "Dimension": "SilverDimensionProcessor",
                "Fact": "SilverFactProcessor",
            },
            "Gold": {
                "Dimension": "GoldDimensionProcessor",
                "Fact": "GoldFactProcessor",
            },
        }
        layer_factory = factory.get(layer, None)
        if not layer_factory:
            return None
        stage_factory = layer_factory.get(stage, None)
        if not stage_factory:
            return None
        return stage_factory


class MetaData:
    """Metadata Utility"""

    @staticmethod
    def generate_json_to_schema_struct_type(json_schema):
        """Generate JSON dict into StructType"""
        schema_factory = {
            "Timestamp": TimestampType,
            "String": StringType,
            "Integer": IntegerType,
            "Long": LongType,
            "Boolean": BooleanType,
            "Double": DoubleType,
            "Float": FloatType,
        }
        schema_builder = []
        for item in json_schema:
            schema_item = StructField(
                f'"{item}"', schema_factory[json_schema[item]](), True
            )
            schema_builder.append(schema_item)
        return StructType(schema_builder)

    @staticmethod
    def generate_metadata_for_runtime(
        stage,
        layer,
        data_source,
        object_name,
        object_type,
        landing_date,
        partition_by,
        save_mode,
        job_id,
        override_table_name,
        skip_validation,
        overwrite_cols,
        environment,
        is_unity_catalog,
    ) -> dict:
        """Generate Meta Data for runtime during the framework usage"""
        is_local = bool(os.environ.get("LOCAL_DEV", False))
        mdl_path = "/mnt/adls/"
        processing_path = "/mnt/processing/StockETL/"
        contract_path_prefix = f"/dbfs{mdl_path}DataContract"
        silver_schema_creation_path = f"/{layer}/{data_source}"
        if is_local:
            contract_path_prefix = str(
                os.path.join(os.getcwd(), "data", "DataContract")
            )
            mdl_path = (
                str(os.path.join(os.getcwd(), "build", "result"))
                .replace("src\\StockETL\\sandbox", "")
                .replace("\\", "/")
                + "/"
            )
        object_path = (
            f"/{layer}/{data_source}/{object_name}/{stage.title()}"
        )
        if layer == "Silver":
            object_path = f"/{layer}/{data_source}/{stage.title()}/{object_name}"
        elif layer == "Gold":
            object_path = f"/{layer}/{data_source}/{stage.title()}/{object_name}"
        processed_full_path = mdl_path + object_path
        datetime_path = f"/{landing_date}"
        file_name = f"/{object_name}"
        contract_path = (
            f"{contract_path_prefix}/{layer}/{data_source}_{object_name}.json"
        )
        partition_by = (
            partition_by.lower() if partition_by != "YYYY,MM,DD" else partition_by
        )

        data_quality_invalid_path = (
            f"/mnt/DataQuality{object_path}{datetime_path}{file_name}.parquet"
        )
        critical_invalid_path = (
            f"/mnt/DataQuality{object_path}{datetime_path}{file_name}_critical.parquet"
        )
        if is_local:
            data_quality_invalid_path = (
                str(
                    os.path.join(
                        os.getcwd(),
                        "build",
                        "result",
                        "DataQuality",
                    )
                    + object_path
                    + datetime_path
                    + f"{file_name}.parquet"
                )
                .replace("src\\StockETL\\sandbox", "")
                .replace("\\", "/")
            )
            critical_invalid_path = (
                str(
                    os.path.join(
                        os.getcwd(),
                        "build",
                        "result",
                        "DataQuality",
                    )
                    + object_path
                    + datetime_path
                    + f"{file_name}_critical.parquet"
                )
                .replace("src\\StockETL\\sandbox", "")
                .replace("\\", "/")
            )
        if override_table_name:
            table_name = override_table_name
        elif is_unity_catalog:
            table_name = f"mdl_seaa_{environment}.{layer.lower()}_{data_source.lower()}.{object_name.lower()}"
        else:
            table_name = f"{data_source.lower()}.{object_name.lower()}"

        if is_unity_catalog:
            uc_external_location = os.environ.get("UC_STORAGE_LOCATION")
            if uc_external_location:
                processed_full_path = uc_external_location + object_path
            else:
                raise RuntimeError(
                    "UC_STORAGE_LOCATION environment variable is required."
                )

        return {
            "layer": layer,
            "stage": stage,
            "data_source": data_source,
            "object_name": object_name,
            "job_id": job_id,
            "object_type": object_type,
            "partition_by": partition_by,
            "landing_date": landing_date,
            "save_mode": save_mode.lower(),
            # Adding Skip Validation in hotfix/silver_skip_validation_develop
            "skip_validation": skip_validation,
            "path": {
                "mdl_path": mdl_path,
                "object_path": object_path,
                "datetime_path": datetime_path,
                "default_landed_path": f"{mdl_path}{object_path.replace('/Processed','/Landed')}{datetime_path}/",
                "full_path": mdl_path
                + object_path
                + datetime_path
                + f"{file_name}.{object_type}",
                "processed_full_path": processed_full_path,
                "contract": contract_path,
                "file_name": f"{file_name}.{object_type}",
                "data_quality": f"{mdl_path}DataQuality",
                "data_catalog": f"{mdl_path}DataCatalog",
                "data_quality_invalid_path": data_quality_invalid_path,
                "critical_invalid_path": critical_invalid_path,
            },
            "database": {
                "schema": data_source.lower(),
                "schema_location": (
                    (mdl_path + silver_schema_creation_path)
                    if layer == "Silver"
                    else (mdl_path + object_path)
                ),
                "table": table_name,
                "source_table": f"{object_name.lower()}_src",
                "partition_by": partition_by,
                "overwrite_cols": overwrite_cols,
            },
            "processing": {
                "extracted_path": f"{processing_path}{object_path}/{datetime.now().strftime('%Y%m%d%H%M%S')}_Extracted.parquet",
                "valid_path": f"{processing_path}{object_path}/{datetime.now().strftime('%Y%m%d%H%M%S')}_Valid.parquet",
                "invalid_path": f"{processing_path}{object_path}/{datetime.now().strftime('%Y%m%d%H%M%S')}_Invalid.parquet",
                "transformed_path": f"{processing_path}{object_path}/{datetime.now().strftime('%Y%m%d%H%M%S')}_Transformed.parquet",
            },
            "reports": {
                "data_quality_columns": [
                    "created",
                    "layer",
                    "stage",
                    "data_source",
                    "object_name",
                    "status",
                    "duplicated",
                    "notqualified",
                    "raw",
                    "transformed",
                    "load",
                    "uuid",
                    "file_ingest",
                    "YYYY",
                    "MM",
                    "DD",
                ],
                "report_partition_by": [
                    "layer",
                    "data_source",
                    "object_name",
                    
                    "stage",
                    "YYYY",
                    "MM",
                    "DD",
                ],
                "data_catalog_columns": [
                    "created",
                    "layer",
                    "stage",
                    
                    "data_source",
                    "object_name",
                    "field_name_c",
                    "dataType",
                    "field_description",
                    "field_validations",
                    "field_is_primary",
                    "Nullable",
                    "completeness",
                    "Distinct Value",
                    "Mean",
                    "Max",
                    "Min",
                    "Sum",
                    "StdDev",
                    "histogram",
                    "uid",
                    "file_ingest",
                    "YYYY",
                    "MM",
                    "DD",
                ],
            },
            "duplicated_keep_option": "last",
        }
