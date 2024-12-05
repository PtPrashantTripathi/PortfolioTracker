import os
import json
from typing import Any, List, Union, Optional
from datetime import datetime

from StockETL.common import Functions
from StockETL.common.CustomLogger import get_logger, with_logging
from StockETL.DataContract.datatypes import DataType

__doc__ = "Contract Module to integrate with StockETL"
logger = get_logger(name="StockETL - DataContract")


class ContractField:
    """Contract Field"""

    def __init__(
        self,
        name: str,
        datatype: Union[str, DataType],
        description: str,
        nullable: Optional[bool] = True,
        primary: Optional[bool] = False,
        validations: Optional[list] = None,
    ):
        """Init"""
        self.name = str(name)
        self.datatype = (
            datatype if isinstance(datatype, DataType) else DataType(datatype)
        )
        self.description = str(description)
        self.nullable = bool(nullable) or True
        self.primary = bool(primary) or False
        self.validations = validations or []

    def get_date_format(self):
        """get_date_format"""
        for x in self.validations:
            if "date_format" in x:
                return x
        return None

    def get_timestamp_format(self):
        """get_timestamp_format"""
        for x in self.validations:
            if "timestamp_format" in x:
                return x
        return None


class Contract:
    """Contract Model to generate to nice json template"""

    def __init__(
        self,
        contract_name: str,
        contract_description: str,
        contract_layer: str,
        contract_stage: str,
        contract_data_source: str,
        contract_object_name: str,
        contract_data_owner: str,
        contract_fields: List[ContractField] = None,
        contract_created_timestamp: str = datetime.now().isoformat(),
        contract_updated_timestamp: str = datetime.now().isoformat(),
        contract_options_override: Optional[dict] = None,
    ):
        """init method"""
        self.contract_name = contract_name
        self.contract_description = contract_description
        self.contract_layer = str(contract_layer).title()
        self.contract_stage = str(contract_stage).title()
        self.contract_data_source = contract_data_source
        self.contract_object_name = contract_object_name
        self.contract_fields = contract_fields
        self.contract_data_owner = self.validator_contract_email_data_owner(
            contract_data_owner
        )
        self.contract_created_timestamp = contract_created_timestamp
        self.contract_updated_timestamp = contract_updated_timestamp
        self.contract_options_override = contract_options_override

    def validator_contract_email_data_owner(self, value):
        """Validation for contract_data_owner must not be null"""
        values = value.split(";") if ";" in value else [value]
        for value in values:
            if not Functions.ContractFunction.validate_email(value):
                raise ValueError(
                    f"CONTRACT: VALIDATION: contract_data_owner must be and email! {value}"
                )
        return value.lower()

    def get_name(self):
        """Get Name"""
        return f"{self.contract_layer}_{self.contract_stage}_{self.contract_name}"

    def generate_struct_schema(self):
        """Generate Struct schema"""
        return StructType(
            [
                StructField(
                    name=str(item.name).lower(),
                    dataType=item.get_datatype(),
                    nullable=bool(item.nullable),
                )
                for item in self.contract_fields
            ]
        )

    def generate_hive_schema(self):
        """Generate hive_schema"""
        data_field_factory = {
            "long": "BIGINT",
        }
        return [
            f"`{item.name}` {data_field_factory.get(item.datatype.lower(), item.datatype.upper())}"
            for item in self.contract_fields
        ]

    def generate_primary_keys(self):
        """Generate primary_keys"""
        primary_keys_list = list(
            filter(lambda x: x.primary is True, self.contract_fields)
        )
        if len(primary_keys_list) == 0:
            return []
        return [x.name.lower() for x in primary_keys_list]

    def generate_cast_integer_expr(self, source_name, target_name, is_try=False):
        return f"""
        {"TRY_" if is_try else ""}CAST(
            REPLACE(
                REPLACE(
                    REGEXP_EXTRACT(REGEXP_REPLACE(`{source_name}`, '^-$', 0), '(([^)]*)\\)'), '(', '-'
                ), ',', ''
            ) AS Integer
        ) {"try_" if is_try else ""}{target_name}
        """

    def generate_cast_date_expr(
        self, source_name, target_name, date_format, is_try=False
    ):
        return f"""
        {"TRY_" if is_try else ""}CAST(
            TO_DATE(TRIM(`{source_name}`), "{date_format}") AS Date
        ) {"try_" if is_try else ""}{target_name}
        """

    def generate_cast_timestamp_expr(
        self, source_name, target_name, timestamp_format, is_try=False
    ):
        return f"""
        {"TRY_" if is_try else ""}CAST(
            TO_TIMESTAMP(TRIM(`{source_name}`), "{timestamp_format}") AS Timestamp
        ) {"try_" if is_try else ""}{target_name}
        """

    def get_cast_expr(self, item: ContractField, is_try=False):
        target_name = item.name
        source_name = target_name
        cast_str = f"""{"TRY_" if is_try else ""}CAST(`{source_name}` AS {item.datatype}) {"try_" if is_try else ""}{target_name}"""
        if item.datatype.lower() == "integer":
            cast_str = self.generate_cast_integer_expr(source_name, target_name, is_try)
        elif item.datatype.lower() == "date":
            date_format = item.get_date_format()
            if date_format != None:
                cast_str = self.generate_cast_date_expr(
                    source_name,
                    target_name,
                    date_format["date_format"]["value"],
                    is_try,
                )
        elif item.datatype.lower() == "timestamp":
            timestamp_format = item.get_timestamp_format()
            if timestamp_format != None:
                cast_str = self.generate_cast_timestamp_expr(
                    source_name,
                    target_name,
                    timestamp_format["timestamp_format"]["value"],
                    is_try,
                )
        return cast_str

    def generate_cast_expr(self):
        """Generate cast_expr for hive"""
        return [self.get_cast_expr(field) for field in self.contract_fields]

    def generate_alias_expr(self):
        """Generate expression to rename columns"""
        return {
            (
                field.field_source_name
                if field.field_source_name != None and field.field_source_name != ""
                else field.name
            ): field.name
            for field in self.contract_fields
        }

    def generate_source_column_list(self):
        """Generate the list of source columns"""
        return [
            (
                field.field_source_name
                if field.field_source_name != None and field.field_source_name != ""
                else field.name
            )
            for field in self.contract_fields
        ]

    @with_logging(logger_module=logger)
    @staticmethod
    def load(path) -> Any:
        """
        Broker to look for contracts
        Checking deployment Folder /mnt/adls/DataContract/layer/data_source/object_name
        """
        # Check in path and parse Contract
        if os.path.exists(path):
            with open(path, encoding="utf-8") as file:
                json_data = json.load(file)
            contract = Contract(**json_data)
            logger.info(
                "Found the contract at parse successful %s - %s",
                contract.contract_data_source,
                contract.contract_object_name,
            )
            return contract
        else:
            raise ValueError(
                f"None Data Contract found for both Landed and Processed : {path}"
            )
