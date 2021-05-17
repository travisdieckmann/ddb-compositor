# MIT License

# Copyright (c) 2021 Travis Dieckmann

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# ==============================================================================

import string  # pylint: disable=W0402
import logging
from os import environ
from typing import List
from enum import IntEnum

from boto3.dynamodb.conditions import Key, Attr

from ddb_compositor.exceptions import UnknownIndexTypeError

logger = logging.getLogger(__name__)
logger.setLevel(environ.get("LOG_LEVEL", logging.INFO))


class IndexType(IntEnum):
    PRIMARY = 100
    GLOBAL_SECONDARY = 90
    LOCAL_SECONDARY = 80


class Index(object):
    """
    DynamoDb Table Index
    """

    def __init__(
        self,
        partition_key_name,
        partition_key_format,
        sort_key_name=None,
        sort_key_format=None,
        index_type: IndexType = None,
        name: str = None,
        composite_separator: str = None,
    ):
        self.partition_key_name = partition_key_name
        self.partition_key_format = partition_key_format
        self.partition_key_format_fields = self.__format_string_field_list(partition_key_format)
        self.sort_key_name = sort_key_name
        self.sort_key_format = sort_key_format
        self.sort_key_format_fields = self.__format_string_field_list(sort_key_format)
        self.type = index_type
        self.name = name
        self.composite_separator = self.__composite_separator(composite_separator)

        pri, gsec, lsec = self.__index_enum_to_bool(index_type)
        self.is_primary = pri
        self.is_global_secondary = gsec
        self.is_local_secondary = lsec

    @staticmethod
    def __index_enum_to_bool(index_type: IndexType) -> List[bool]:
        if index_type not in list(map(int, IndexType)) or index_type is None:
            raise UnknownIndexTypeError

        return (
            (index_type == IndexType.PRIMARY),
            (index_type == IndexType.GLOBAL_SECONDARY),
            (index_type == IndexType.LOCAL_SECONDARY),
        )

    def __composite_separator(self, separator):
        if any((self.partition_key_format_fields, self.sort_key_format_fields)) == (separator is None):
            raise RuntimeError(
                "composite_separator must be specified when declaring partition_key_format or sort_key_format strings"
            )
        else:
            return separator

    def partition_key_value(self, field_values: dict) -> dict:
        """Match field_values with fields in the hash-key format string

        Arguments:
            field_values {dict} -- Dictionary of item fields and values

        Returns:
            dict -- Hash-key fields and values
        """
        return {self.partition_key_name: self.partition_key_format.format(**field_values)}

    def sort_key_value(self, field_values: dict) -> dict:
        """Match field_values with fields in the range-key format string

        Arguments:
            field_values {dict} -- Dictionary of item fields and values

        Returns:
            dict -- Range-key fields and values
        """
        if self.sort_key_name is None:
            return {}
        return {self.sort_key_name: self.sort_key_format.format(**field_values)}

    def full_key(self, field_values: dict) -> dict:
        """Match field_values with fields in the combined hash and range key's format strings

        Arguments:
            field_values {dict} -- Dictionary of item fields and values

        Returns:
            dict -- hash and range key's fields and values
        """
        key = self.partition_key_value(field_values)
        key.update(self.sort_key_value(field_values))
        return key

    def get_ne_conditional(self, field_values: dict) -> Attr:
        """Assemble index not-equals conditional statement

        Arguments:
            field_values {dict} -- Dictionary of item fields and values

        Returns:
            Attr -- Conditional attribute
        """
        expression = Attr(self.partition_key_name).ne(self.partition_key_format.format(**field_values))

        if self.sort_key_name is not None:
            expression = expression & Attr(self.sort_key_name).ne(self.sort_key_format.format(**field_values))

        return expression

    def __format_string_field_list(self, format_string: str) -> list:
        format_string = format_string or ""
        parsed_list = list(string.Formatter().parse(format_string))
        return [x[1] for x in parsed_list if x[1] != None and x[1] != ""]

    @staticmethod
    def __ordered_intersection(source_list: list, match_list: list):
        matches = []

        for item in source_list:
            if item in match_list:
                matches.append(item)

        return matches

    def partition_key_ordered_intersection(self, field_values: dict) -> list:
        return self.__ordered_intersection(
            self.partition_key_format_fields,
            field_values.keys(),
        )

    def sort_key_ordered_intersection(self, field_values: dict) -> list:
        return self.__ordered_intersection(self.sort_key_format_fields, field_values.keys())

    def field_values_intersection(self, field_values: dict) -> dict:
        return {
            key: field_values[key]
            for key in set(self.partition_key_format_fields + self.sort_key_format_fields).intersection(
                field_values.keys()
            )
        }

    def query_score(self, field_values: dict, unique_id_attribute_name: str = None) -> int:
        score = 0
        matching_fields = self.__ordered_intersection(
            self.partition_key_format_fields,
            field_values.keys(),
        )

        if len(matching_fields) < len(self.partition_key_format_fields):
            return round(score)

        if unique_id_attribute_name in self.partition_key_format_fields and unique_id_attribute_name in field_values:
            score += 200 * (
                (
                    len(self.partition_key_format_fields)
                    - self.partition_key_format_fields.index(unique_id_attribute_name)
                )
                / len(self.partition_key_format_fields)
            )

        matching_fields = self.__ordered_intersection(self.sort_key_format_fields, field_values.keys())

        if len(self.sort_key_format_fields) < 1 or len(matching_fields) < 1:
            return round(score)

        for field in self.sort_key_format_fields:
            if field in field_values:
                score += 1
            else:
                break

        score = score / len(self.sort_key_format_fields) * 100
        if unique_id_attribute_name in self.sort_key_format_fields and unique_id_attribute_name in field_values:
            score += 100 * (
                (len(self.sort_key_format_fields) - self.sort_key_format_fields.index(unique_id_attribute_name))
                / len(self.sort_key_format_fields)
            )

        return round(score)

    def get_sort_best_match(self, field_values: dict) -> str:
        best_match = ""

        for segment in string.Formatter().parse(self.sort_key_format):
            best_match += segment[0]
            field = segment[1]
            if field in field_values:
                best_match += str(field_values[field])
            else:
                break

        return best_match

    def get_condition_expression(
        self,
        field_values: dict,
        key_score: int,
        force_key_begins_with: bool = False,
    ) -> Key:
        key = Key(self.partition_key_name).eq(self.partition_key_format.format(**field_values))

        if self.sort_key_format is None:
            logger.debug("No sort_key present. Condition expression only includes %s", self.partition_key_name)
            return key

        if key_score == 100 and not force_key_begins_with:
            key = key & Key(self.sort_key_name).eq(self.sort_key_format.format(**field_values))
        else:
            sort_best_match = self.get_sort_best_match(field_values)
            if force_key_begins_with:
                sort_best_match = sort_best_match.rstrip(self.composite_separator)
            key = key & Key(self.sort_key_name).begins_with(sort_best_match)

        return key
