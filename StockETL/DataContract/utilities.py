import re
import json
import string
from typing import Union

import pandas as pd

from . import Contract, ContractField
from ..common.CustomLogger import logging, get_logger, with_debug_logging

logger = get_logger("Utilities - CreateContract", level=logging.DEBUG)


class CreateContract:
    """Utilities to create Data Contract"""

    CLEANUP_COLS = [
        "Requestor Detail & Owner Details",
        "Source Details",
        "Ingestion design pattern",
        "Override Options",
    ]
    LOGIC_FACTORY = {
        "isalphanumeric": {
            "regex": r"""(isalphanumeric)""",
            "group": 0,
            "value_type": str,
            "compute_value": "'[^A-Za-z0-9]'",
        },
        "isnumeric": {
            "regex": r"""(isnumeric)""",
            "group": 0,
            "value_type": str,
            "compute_value": "'[^A-Za-z]'",
        },
        "isalpha": {
            "regex": r"""(isalpha)""",
            "group": 0,
            "value_type": str,
            "compute_value": "'[^0-9]'",
        },
        "date_format": {
            "regex": r"""(date_format)( = )([\"|\'].*[\"|\'])""",
            "group": 2,
            "value_type": str,
        },
        "string_format": {
            "regex": r"""(string_format)( = )([\"|\'].*[\"|\'])""",
            "group": 2,
            "value_type": str,
        },
        "date_begin": {
            "regex": r"""(date_begin) ((>=)|(>)) """,
            "group": 2,
            "value_type": str,
        },
        "date_end": {
            "regex": r"""(date_end) ((<=)|(<)) """,
            "group": 1,
            "value_type": str,
        },
        "date_range": {
            "regex": r"""(date_between)( between )""",
            "group": 1,
            "value_type": str,
        },
        "operator": {
            "regex": r"""(<>)|(==)|(!=)|(>=)|(>)|(<=)|(<)""",
            "group": 0,
            "value_type": float,
        },
    }

    def _split_string_into_list_by_character(
        self, input_string: str, split_string: str
    ) -> list:
        """Split string by character"""
        return [x.strip() for x in input_string.split(split_string)]

    def _find_logic_factory(self, input_string: str):
        """Find logic factory"""
        for key, logic in self.LOGIC_FACTORY.items():
            if re.search(logic["regex"], input_string):
                logger.info("Found %s - for field %s", key, input_string)
                return logic
        return None

    def _build_logic_string_to_dict(self, item: str, field: ContractField) -> list:
        """Build logic string to dict"""
        item_result = {}
        logic_parser = self._find_logic_factory(item)
        if not logic_parser:
            return None
        match_string = re.search(logic_parser["regex"], item).group(
            logic_parser["group"]
        )
        key, value = self._split_string_into_list_by_character(item, match_string)
        if key == "":
            key = "computed"
        if logic_parser.get("compute_value", None):
            value = logic_parser.get("compute_value")
            match_string = "regex"
        else:
            value = self._replace_quote_from_string(value)
        item_result[key] = {
            "operator": match_string,
            "value": value,
        }
        return item_result

    def _coalesce_field_name(self, field_name: str, field_dict: dict) -> string:
        """get value from field_dict"""
        # print(field_name, "-", field_dict.get(field_name))
        if field_name == "logic":
            return field_dict.get(field_name, None)
        if field_name == "primary_key":
            return field_dict.get(f"source_{field_name}", "n")
        return field_dict.get(
            f"target_{field_name}", field_dict[f"source_{field_name}"]
        )

    def _parse_validation_rules_to_field(
        self, field: ContractField, logics: Union[str, list]
    ):
        """apply validation rule to fields"""
        if not logics:
            return field
        logics = (
            [x.strip().strip('"') for x in logics.split(",")]
            if not isinstance(logics, list)
            else [logics]
        )
        print(logics)
        if field.field_is_primary == "y":
            logics.append("is_primary")
        for single_logic in logics:
            field, parse_logic = self._apply_validations_rules(field, single_logic)
            if parse_logic:
                field.field_validations.append(parse_logic)
        return field

    def _apply_validations_rules(self, field: ContractField, logic: str):
        if "notnull" in logic:
            field.field_nullable = False
            logic = {"is_nullable": {"level": "ERROR", "value": False}}
            return field, logic
        if "is_primary" in logic:
            field.field_is_primary = True
            field.field_nullable = False
            logic = {"is_nullable": {"level": "CRITICAL", "value": False}}
            return field, logic
        print(logic)
        logic = self._build_logic_string_to_dict(item=logic, field=field)
        print("-", logic)
        return field, logic

    @with_debug_logging(logger_module=logger)
    def _clean_up_field_value(self, df: pd):
        return [
            {
                k: v if k == "logic" else self._clean_up_string(v)
                for k, v in m.items()
                if pd.notnull(v)
            }
            for m in df.to_dict(orient="records")
        ]

    @with_debug_logging(logger_module=logger)
    def _build_contract_fields(self, df: pd):
        result = []
        raw_field_list = [
            {k: v for k, v in m.items() if pd.notnull(v)}
            for m in df.to_dict(orient="records")
        ]
        field_list = self._clean_up_field_value(df=df)
        for row, raw in zip(field_list, raw_field_list):
            # print(raw)
            item_primary_key = False
            item_nullable = True
            if self._coalesce_field_name("primary_key", row) == "y":
                item_primary_key = True
                item_nullable = False
            item = ContractField(
                field_name=self._coalesce_field_name("attribute", row),
                field_type=self._coalesce_field_name("datatype", row),
                field_nullable=item_nullable,
                field_is_primary=item_primary_key,
                field_description=self._coalesce_field_name("description", row),
            )
            item_logic = self._coalesce_field_name("logic", row)
            if not item_logic:
                del item.field_validations
            item = self._parse_validation_rules_to_field(field=item, logics=item_logic)
            result.append(item)
        return result

    def _replace_punctuation_from_string(self, unclean_string):
        """Remove punctuation from string"""
        # Remove _ as it replace space
        regex_escape_string = r"""!"#$%&'()*+,-./:;<=>?@[\]^`{|}~"""
        regex_remove_punctuation = re.compile("[%s]" % re.escape(regex_escape_string))
        return regex_remove_punctuation.sub("", unclean_string)

    def _replace_quote_from_string(self, unclean_string):
        """Remove single and double quotes from string"""
        # Remove _ as it replace space
        if not unclean_string or unclean_string == "None":
            return None
        if isinstance(unclean_string, bool):
            return unclean_string
        regex_escape_string = r""""'"""
        regex_remove_punctuation = re.compile("[%s]" % re.escape(regex_escape_string))
        return regex_remove_punctuation.sub("", unclean_string)

    def _clean_up_string(self, unclean_string):
        """Clean single string"""
        clean_string = (
            self._replace_punctuation_from_string(unclean_string)
            .replace(" ", "_")
            .lower()
        )
        # print(unclean_string, " - ", clean_string)
        return clean_string

    def _clean_up_columns(self, col_list):
        """Clean up punctuation from col name"""
        return [self._clean_up_string(col) for col in col_list]

    @with_debug_logging(logger_module=logger)
    def _load_sheet_name_from_excel(
        self,
        excel_file,
        sheet_name: str = "Sheet1",
        usecols: str = "A:A",
        header: int = 0,
    ) -> pd:
        df = pd.read_excel(
            excel_file, sheet_name=sheet_name, usecols=usecols, header=header
        )
        df.columns = self._clean_up_columns(df.columns)
        return df

    def _transpose_df_and_promote_header(self, df):
        df = df.transpose().reset_index().dropna(axis=1)
        df.columns = df.iloc[0]
        df.drop(index=0, axis=0, inplace=True)
        # df.columns = self._clean_up_columns(df.columns)
        return df

    @with_debug_logging(logger_module=logger)
    def build_overide_options(self, json_data: dict) -> dict:
        """build override options"""
        columns_dont_keep = [
            "Owner email",
            "Department",
            "Source",
            "Data Name",
            "Scope",
            "Description",
            "Source type",
            "Extraction method",
            "Layer",
            "Trigger",
            "Type of Load",
            "owner_email",
            "department",
            "source",
            "data_name",
            "scope",
            "description",
            "source_type",
            "extraction_method",
            "layer",
            "trigger",
            "type_of_load",
            "requestor_detail__owner_details",
        ]
        data = {}
        for key, value in json_data.items():
            value = self._replace_quote_from_string(value)
            if key == "escape":
                data["escape"] = '"'
                continue
            if not value:
                continue
            if key in columns_dont_keep:
                continue
            data[key] = value
        return data

    @with_debug_logging(logger_module=logger)
    def create_from_excel(self, path):
        """Generate from Excel"""
        excel_file = pd.ExcelFile(path, engine="openpyxl")
        change_log_df = self._load_sheet_name_from_excel(
            excel_file=excel_file, sheet_name="Change Log", header=1, usecols="A:E"
        )
        meta_data_df_raw = self._load_sheet_name_from_excel(
            excel_file=excel_file,
            sheet_name="Details & Approval",
            header=5,
            usecols="B:C",
        )
        meta_data_df = self._transpose_df_and_promote_header(meta_data_df_raw)
        meta_data_override = json.loads(meta_data_df.to_json(orient="records"))[0]
        override_options = self.build_overide_options(meta_data_override)
        cols_clean_up = [
            x
            for x in self._clean_up_columns(self.CLEANUP_COLS)
            if x in meta_data_df.columns
        ]
        meta_data_df.drop(labels=cols_clean_up, axis=1, inplace=True)
        meta_data_df.columns = self._clean_up_columns(meta_data_df.columns)
        meta_data = json.loads(meta_data_df.to_json(orient="records"))[0]
        fields_df = self._load_sheet_name_from_excel(
            excel_file, sheet_name="DMR", header=1, usecols="A:N"
        )
        contract_fields = self._build_contract_fields(df=fields_df)
        print(meta_data)

        contract = Contract(
            contract_description=meta_data["description"],
            contract_name=f'{meta_data["department"]} - {meta_data["layer"]} - {meta_data["source"]} - {meta_data["data_name"]}',
            contract_layer=meta_data["layer"],
            contract_stage="Landed",
            contract_data_source=meta_data["source"],
            contract_object_name=meta_data["data_name"],
            contract_data_owner=meta_data["owner_email"],
            contract_fields=contract_fields,
            contract_options_override=override_options,
        )
        return contract

    def to_json(self, contract):
        """To JSON unitlities"""
        # return json.dumps(contract, sort_keys=True, indent=4)
        return json.dumps(contract, indent=4)

    def __str__(self):
        return "Utility - CreateContract"
