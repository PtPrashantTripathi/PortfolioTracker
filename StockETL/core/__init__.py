import json
import importlib.metadata
from uuid import uuid4
from typing import Any, Optional

from pydantic import Field, BaseModel, ConfigDict, field_validator
from pyspark.sql import DataFrame

from .common import Constants, Functions, CustomSpark
from .DataContract import ContractBroker
from .common.CustomLogger import get_logger, with_logging
from .DataReport.BaseReport import DataReport
from .DataOnboarder.Processor.Gold import Fact as GoldFact
from .DataOnboarder.Processor.Gold import Dimension as GoldDimension
from .DataOnboarder.Processor.Bronze import Landed as BronzeLanded
from .DataOnboarder.Processor.Bronze import Processed as BronzeProcessed
from .DataOnboarder.Processor.Silver import Fact as SilverFact
from .DataOnboarder.Processor.Silver import Dimension as SilverDimension
from .DataOnboarder.Processor.FileReader import FileReader
from .DataOnboarder.Processor.BaseProcessor import StructType, BaseProcessor

# module level doc-string
__doc__ = """
 -  Data On-Boarders
=====================================================================

**** is a Python package providing fast, flexible, and expressive data
structures designed to make working with "relational" or "labeled" data both
easy and intuitive. It aims to be the fundamental high-level building block for
doing practical, **real world** data analysis in Python. Additionally, it has
the broader goal of becoming **the most powerful and flexible open source data
analysis / manipulation tool available in any language**. It is already well on
its way toward this goal.

Main Features
-------------
Here are just a few of the things that  does well:

    - Easy handling of missing data in floating point as well as non-floating
      point data.
    - Size mutability: columns can be inserted and deleted from DataFrame and
      higher dimensional objects
    - Automatic and explicit data alignment: objects can be explicitly aligned
      to a set of labels, or the user can simply ignore the labels and let
      `Series`, `DataFrame`, etc. automatically align the data for you in
      computations.
    - Powerful, flexible group by functionality to perform split-apply-combine
      operations on data sets, for both aggregating and transforming data.
    - Make it easy to convert ragged, differently-indexed data in other Python
      and NumPy data structures into DataFrame objects.
    - Intelligent label-based slicing, fancy indexing, and subsetting of large
      data sets.
    - Intuitive merging and joining data sets.
    - Flexible reshaping and pivoting of data sets.
    - Hierarchical labeling of axes (possible to have multiple labels per tick).
    - Robust IO tools for loading data from flat files (CSV and delimited),
      Excel files, databases, and saving/loading data from the ultrafast HDF5
      format.
    - Time series-specific functionality: date range generation and frequency
      conversion, moving window statistics, date shifting and lagging.
"""
# CONSTANT
MDL_PATH = "/mnt/adls"

# SETUP LOGGING DECORATOR
logger = get_logger("")


class(BaseModel):
    """ Data On-Boarder Framework main function."""


    layer: Optional[str] = Constants.Layer.BRONZE.value
    stage: Optional[str] = None
    data_source: str
    object_name: str
    file_path: Optional[Any] = None
    job_id: Optional[str] = Field(default=f"{uuid4()}")  # For data
    landing_date: Optional[str] = "2099-01-01"
    save_mode: Optional[str] = "overwrite"
    overwrite_cols: Optional[str] = None
    object_type: Optional[str] = "parquet"  # default landed/parquet and processed/delta
    partition_by: Optional[str] = "YYYY,MM,DD"
    table_name: Optional[str] = None
    debug: Optional[bool] = False
    sequential_extraction: Optional[bool] = False
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow", fronze=False)
    skip_validation: Optional[bool] = False
    environment: Optional[str] = None
    metastore: Optional[str] = Constants.Metastore.HIVE.value
    cache_level: Optional[str] = "NONE"

    def __init__(self, **data):
        super().__init__(**data)
        self.spark = CustomSpark.get_spark_session()
        self.custom_steps = {"EXTRACT": [], "TRANSFORM": [], "LOAD": []}
        self.runtime_report = {}
        try:
            self.__version__ = importlib.metadata.verion("")
        except:
            self.__version__ = "4.0.3"
        logger.info(f"USING VERSION - {self.__version__}")


    @field_validator("layer")
    def validator_layer(cls, value):
        """Validation for layer must not be null"""
        if not Constants.Layer.__contains__(value.lower().title()):
            raise ValueError(
                "Layer is not configured to run . Please contact support at vn.de.dna@unilever.com"
            )
        return value.lower().title()

    @field_validator("stage")
    def validator_stage(cls, value):
        """Validation for stage must not be null"""
        if not value:
            return None
        return value.lower().title()

    @field_validator("data_source")
    def validator_data_source(cls, value):
        """Validation for data_source"""
        if not value:
            raise ValueError("Data_Source is null. Please fill this in.")
        return value

    @field_validator("object_name")
    def validator_object_name(cls, value):
        """Validation for object_name"""
        if not value:
            raise ValueError("object_name is null. Please fill this in.")
        return value

    @field_validator("partition_by")
    def validator_partition_by(cls, value):
        """Validation for object_name"""
        if not value:
            return "YYYY,MM,DD"
        if "YYYY,MM,DD" in value:
            return value
        return f"{value}"

    @field_validator("file_path")
    def validator_file_path(cls, value):
        """Validation for file_path"""

        def clean_file_path_blob_storage(file_path):
            """Sub function to clean up file_path"""
            if file_path.startswith("/mnt/"):
                return file_path
            if file_path.startswith("/dbfs/"):
                file_path = file_path.replace("/dbfs/", "")
            if file_path.startswith("processing/"):
                file_path = f"/mnt/{file_path}"
            if not file_path.startswith("/"):
                file_path = f"/{file_path}"

            return file_path

        logger.info(value)
        if not value:
            return None
        if isinstance(value, DataFrame):
            return value
        if isinstance(value, list):
            value = [clean_file_path_blob_storage(x) for x in value]
        if "," in value:
            value = [clean_file_path_blob_storage(x) for x in value.split(",")]
        return value

    @field_validator("save_mode")
    def validator_save_mode(cls, value) -> str:
        """Validation for save_mode. Return append when the file_path is a list of file"""
        return Constants.SaveMode.__getitem__(value.upper()).value

    @field_validator("landing_date")
    def validator_landing_date(cls, value):
        """Validation for landing_date"""
        if not value:
            raise ValueError("Landing Date is not present. Please redefine it")
        return Functions.Helper.Datetime.convert_string_to_date(value, fmt="%Y-%m-%d")

    @field_validator("environment")
    def validator_environment(cls, value) -> str:
        """Validation for Envinronment must not be null"""
        if value and not Constants.Environment.__contains__(value.lower()):
            raise ValueError(
                "Environment is not correct! It should be DEV, QA or Prod only."
            )
        return value.lower()

    @field_validator("metastore")
    def validator_metastore(cls, value) -> str:
        """Validation for Envinronment must not be null"""
        if value and not Constants.Metastore.__contains__(value.lower()):
            raise ValueError(
                "Metastore type is not correct! It should be `hive` or `unity_catalog` only."
            )
        return value.lower()

    def add_custom_function(
        self, col_name: str, function: Any, location: str, params: Any
    ) -> None:
        """Add custom functions"""
        if not Constants.UdfLocation.__contains__(location.upper()):
            raise ValueError(f"Invalid for custom function location {location}")
        item = {"col_name": col_name, "function_name": function, "params": params}
        self.custom_steps[location.split("_")[-1].upper()].append(item)

    def get_runtime_report(self):
        """Get Runtime report"""
        return self.runtime_report

    def get_files_ingest_in_batch(self):
        """Get File ingest into meta_data"""
        if isinstance(self.file_path, DataFrame):
            return f"Spark DataFrame with {self.file_path.count()}"
        if isinstance(self.file_path, list):
            return ",".join(self.file_path)
        return self.file_path

    def get_app_runtime(self):
        """Return Runtime info"""
        object_list = [
            "stage",
            "layer",
            "data_source",
            "object_name",
            "landing_date",
            "partition_by",
            "save_mode",
            "job_id",
        ]
        result = "RUNTIME - INITIALIZATION REPORT - " + " - ".join(
            self.meta_data[x] for x in object_list
        )
        result += f' - {self.meta_data["database"]["table"]} - {self.meta_data["path"]["contract"]}'
        return result

    @with_logging(logger_module=logger)
    def prepare_for_runtime(self):
        """Prepare for Runtime"""
        # Get the environment
        if not self.environment:  # environment is None

            def __envs_dict(environment):
                envs_dict = {
                    "Development": Constants.Environment.DEV.value,
                    "QA": Constants.Environment.QA.value,
                    "Production": Constants.Environment.PROD.value,
                }
                return envs_dict[environment]

            cluster_tags_str = self.spark.conf.get(
                "spark.databricks.clusterUsageTags.clusterAllTags"
            )
            cluster_tags_list = json.loads(cluster_tags_str)
            environment_list = [
                tag["value"] for tag in cluster_tags_list if tag["key"] == "Environment"
            ]
            if len(environment_list) == 0:
                raise RuntimeError(
                    "Could not recognize the Environment. Please either define the environment parameter or check the cluster's tags."
                )
            self.environment = __envs_dict(environment_list[0])
        # Generate runtime meta_data
        self.meta_data = Functions.Helper.MetaData.generate_metadata_for_runtime(

            stage=self.stage,
            layer=self.layer,
            data_source=self.data_source,
            object_name=self.object_name,
            object_type=self.object_type,
            landing_date=self.landing_date,
            partition_by=self.partition_by,
            save_mode=self.save_mode,
            job_id=self.job_id,
            override_table_name=self.table_name,
            skip_validation=self.skip_validation,
            overwrite_cols=self.overwrite_cols,
            environment=self.environment,
            is_unity_catalog=self.metastore.lower()
            == Constants.Metastore.UNITY_CATALOG.value,
        )
        # Check if initialized has file path, or auto switch to landed default file location on that day
        if not self.file_path:
            self.file_path = f"""{self.meta_data["path"]["default_landed_path"]}"""
        self.meta_data["file_ingest"] = self.get_files_ingest_in_batch()

        # Check for contract
        self.contract = ContractBroker().check_contract(
            self.meta_data["path"]["contract"]
        )
        logger.info(self.get_app_runtime())
        # Generate addition data from contract
        self.meta_data["schema"] = {
            "struct": self.contract.generate_struct_schema(),
            "root": self.contract.generate_hive_schema(),
            "primary_keys": self.contract.generate_primary_keys(),
            "cast_expr": self.contract.generate_cast_expr(),
            "alias_expr": self.contract.generate_alias_expr(),
        }

    def log_runtime(self):
        logger.info(f"FINAL - INGEST - {len(self.runtime_report)} files")
        if len(self.runtime_report) > 1:
            for item in self.runtime_report:
                logger.info(f"FINAL - {item.upper()} : {self.runtime_report[item]}")
        else:
            logger.info(f"FINAL - {self.runtime_report}")

    @with_logging(logger_module=logger)
    def processor_factory(self, name):
        factory = {
            "BronzeLanded": BronzeLanded.BronzeLandedProcessor,
            "BronzeProcessed": BronzeProcessed.BronzeProcessedProcessor,
            "SilverDimension": SilverDimension.SilverDimensionProcessor,
            "SilverFact": SilverFact.SilverFactProcessor,
            "GoldDimension": GoldDimension.GoldDimensionProcessor,
            "GoldFact": GoldFact.GoldFactProcessor,
        }
        processor = factory.get(name, None)
        if not processor:
            raise NotImplementedError(f"{name} Processor is not yet Implemented")
        logger.info(f"Assigned Processor - {processor.__name__}")
        return processor

    def create_dummy_empty_dataframe(self):
        return self.spark.createDataFrame([], schema=StructType([]))

    @with_logging(logger_module=logger)
    def run(self):
        """Run itself"""
        self.prepare_for_runtime()
        # Assign processor
        processor = self.processor_factory(f"{self.layer}{self.stage}")
        reader = FileReader(self.file_path, self.contract.contract_options_override)

        self.processor = processor(
            spark=self.spark,
            input_data=self.file_path,
            reader=reader,
            custom_udf=self.custom_steps,
            meta_data=self.meta_data,
            contract=self.contract,
            sequential_extraction=self.sequential_extraction,
            cache_level=self.cache_level,
        )
        status = True
        ouput_df = self.create_dummy_empty_dataframe()
        transform_df = self.create_dummy_empty_dataframe()

        try:
            self.processor.extract()
            self.processor.validate()
            self.processor.transform()
            self.processor.load()
        except ValidationError as validation_error:
            status = False
            logger.error(validation_error)
            raise validation_error
        except ValueError as value_error:
            status = False
            logger.error(value_error)
            ouput_df = self.processor.get_transformed()
            transform_df = self.processor.get_transformed()
            raise value_error
        else:
            ouput_df = self.processor.get_transformed()
            transform_df = self.processor.get_transformed()
            status = True
        finally:
            # Generate data reports
            reports = DataReport(
                input_df=self.processor.get_extracted(),
                output_df=ouput_df,
                error_df=self.processor.get_invalid(),
                transform_df=transform_df,
                meta_data=self.meta_data,
                contract=self.contract,
                spark=self.spark,
                status=status,
            )
            self.runtime_report = reports.add_runtime()
            reports.add_data_quality()
            reports.add_data_catalog()
            reports.commit()
            self.log_runtime()

    def get_error_records_as_json(self):
        return (
            self.processor.get_invalid()
            .limit(1000)
            .toJSON()  # get first 1000 invalid records only
            .map(lambda str_json: json.loads(str_json))
            .collect()
        )

    def get_extract_file_result_as_json(self):  # for BronzeLandedProcessor
        try:
            return (
                self.processor.extract_result_df.limit(1000)
                .toJSON()  # get first 1000 invalid records only
                .map(lambda str_json: json.loads(str_json))
                .collect()
            )
        except Exception:
            raise Exception(
                f"Unsupported to get extraction result at {self.layer} - {self.stage}"
            )

    def get_owner_email(self):
        return {
            "data_owner": self.contract.contract_data_owner,
            "process_owner": self.contract.contract_process_owner,
        }
