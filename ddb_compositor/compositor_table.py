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

import json
import string
import logging
from os import environ
from decimal import Decimal
from datetime import datetime, date
from typing import List, Union
from copy import deepcopy

import boto3
import botocore.exceptions
from boto3.dynamodb.transform import TypeSerializer, TypeDeserializer

from ddb_compositor.indexes import (
    PrimaryIndex,
    GlobalSecondaryIndex,
    LocalSecondaryIndex,
)
from ddb_compositor.utility import DdbJsonEncoder
from ddb_compositor.exceptions import UnknownIndexTypeError

logger = logging.getLogger(__name__)
logger.setLevel(environ.get("LOG_LEVEL", logging.INFO))
# https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html
# fmt: off
RESERVED_WORDS = ['ABORT','ABSOLUTE','ACTION','ADD','AFTER','AGENT','AGGREGATE','ALL','ALLOCATE','ALTER','ANALYZE','AND','ANY','ARCHIVE','ARE','ARRAY','AS','ASC','ASCII','ASENSITIVE','ASSERTION','ASYMMETRIC','AT','ATOMIC','ATTACH','ATTRIBUTE','AUTH','AUTHORIZATION','AUTHORIZE','AUTO','AVG','BACK','BACKUP','BASE','BATCH','BEFORE','BEGIN','BETWEEN','BIGINT','BINARY','BIT','BLOB','BLOCK','BOOLEAN','BOTH','BREADTH','BUCKET','BULK','BY','BYTE','CALL','CALLED','CALLING','CAPACITY','CASCADE','CASCADED','CASE','CAST','CATALOG','CHAR','CHARACTER','CHECK','CLASS','CLOB','CLOSE','CLUSTER','CLUSTERED','CLUSTERING','CLUSTERS','COALESCE','COLLATE','COLLATION','COLLECTION','COLUMN','COLUMNS','COMBINE','COMMENT','COMMIT','COMPACT','COMPILE','COMPRESS','CONDITION','CONFLICT','CONNECT','CONNECTION','CONSISTENCY','CONSISTENT','CONSTRAINT','CONSTRAINTS','CONSTRUCTOR','CONSUMED','CONTINUE','CONVERT','COPY','CORRESPONDING','COUNT','COUNTER','CREATE','CROSS','CUBE','CURRENT','CURSOR','CYCLE','DATA','DATABASE','DATE','DATETIME','DAY','DEALLOCATE','DEC','DECIMAL','DECLARE','DEFAULT','DEFERRABLE','DEFERRED','DEFINE','DEFINED','DEFINITION','DELETE','DELIMITED','DEPTH','DEREF','DESC','DESCRIBE','DESCRIPTOR','DETACH','DETERMINISTIC','DIAGNOSTICS','DIRECTORIES','DISABLE','DISCONNECT','DISTINCT','DISTRIBUTE','DO','DOMAIN','DOUBLE','DROP','DUMP','DURATION','DYNAMIC','EACH','ELEMENT','ELSE','ELSEIF','EMPTY','ENABLE','END','EQUAL','EQUALS','ERROR','ESCAPE','ESCAPED','EVAL','EVALUATE','EXCEEDED','EXCEPT','EXCEPTION','EXCEPTIONS','EXCLUSIVE','EXEC','EXECUTE','EXISTS','EXIT','EXPLAIN','EXPLODE','EXPORT','EXPRESSION','EXTENDED','EXTERNAL','EXTRACT','FAIL','FALSE','FAMILY','FETCH','FIELDS','FILE','FILTER','FILTERING','FINAL','FINISH','FIRST','FIXED','FLATTERN','FLOAT','FOR','FORCE','FOREIGN','FORMAT','FORWARD','FOUND','FREE','FROM','FULL','FUNCTION','FUNCTIONS','GENERAL','GENERATE','GET','GLOB','GLOBAL','GO','GOTO','GRANT','GREATER','GROUP','GROUPING','HANDLER','HASH','HAVE','HAVING','HEAP','HIDDEN','HOLD','HOUR','IDENTIFIED','IDENTITY','IF','IGNORE','IMMEDIATE','IMPORT','IN','INCLUDING','INCLUSIVE','INCREMENT','INCREMENTAL','INDEX','INDEXED','INDEXES','INDICATOR','INFINITE','INITIALLY','INLINE','INNER','INNTER','INOUT','INPUT','INSENSITIVE','INSERT','INSTEAD','INT','INTEGER','INTERSECT','INTERVAL','INTO','INVALIDATE','IS','ISOLATION','ITEM','ITEMS','ITERATE','JOIN','KEY','KEYS','LAG','LANGUAGE','LARGE','LAST','LATERAL','LEAD','LEADING','LEAVE','LEFT','LENGTH','LESS','LEVEL','LIKE','LIMIT','LIMITED','LINES','LIST','LOAD','LOCAL','LOCALTIME','LOCALTIMESTAMP','LOCATION','LOCATOR','LOCK','LOCKS','LOG','LOGED','LONG','LOOP','LOWER','MAP','MATCH','MATERIALIZED','MAX','MAXLEN','MEMBER','MERGE','METHOD','METRICS','MIN','MINUS','MINUTE','MISSING','MOD','MODE','MODIFIES','MODIFY','MODULE','MONTH','MULTI','MULTISET','NAME','NAMES','NATIONAL','NATURAL','NCHAR','NCLOB','NEW','NEXT','NO','NONE','NOT','NULL','NULLIF','NUMBER','NUMERIC','OBJECT','OF','OFFLINE','OFFSET','OLD','ON','ONLINE','ONLY','OPAQUE','OPEN','OPERATOR','OPTION','OR','ORDER','ORDINALITY','OTHER','OTHERS','OUT','OUTER','OUTPUT','OVER','OVERLAPS','OVERRIDE','OWNER','PAD','PARALLEL','PARAMETER','PARAMETERS','PARTIAL','PARTITION','PARTITIONED','PARTITIONS','PATH','PERCENT','PERCENTILE','PERMISSION','PERMISSIONS','PIPE','PIPELINED','PLAN','POOL','POSITION','PRECISION','PREPARE','PRESERVE','PRIMARY','PRIOR','PRIVATE','PRIVILEGES','PROCEDURE','PROCESSED','PROJECT','PROJECTION','PROPERTY','PROVISIONING','PUBLIC','PUT','QUERY','QUIT','QUORUM','RAISE','RANDOM','RANGE','RANK','RAW','READ','READS','REAL','REBUILD','RECORD','RECURSIVE','REDUCE','REF','REFERENCE','REFERENCES','REFERENCING','REGEXP','REGION','REINDEX','RELATIVE','RELEASE','REMAINDER','RENAME','REPEAT','REPLACE','REQUEST','RESET','RESIGNAL','RESOURCE','RESPONSE','RESTORE','RESTRICT','RESULT','RETURN','RETURNING','RETURNS','REVERSE','REVOKE','RIGHT','ROLE','ROLES','ROLLBACK','ROLLUP','ROUTINE','ROW','ROWS','RULE','RULES','SAMPLE','SATISFIES','SAVE','SAVEPOINT','SCAN','SCHEMA','SCOPE','SCROLL','SEARCH','SECOND','SECTION','SEGMENT','SEGMENTS','SELECT','SELF','SEMI','SENSITIVE','SEPARATE','SEQUENCE','SERIALIZABLE','SESSION','SET','SETS','SHARD','SHARE','SHARED','SHORT','SHOW','SIGNAL','SIMILAR','SIZE','SKEWED','SMALLINT','SNAPSHOT','SOME','SOURCE','SPACE','SPACES','SPARSE','SPECIFIC','SPECIFICTYPE','SPLIT','SQL','SQLCODE','SQLERROR','SQLEXCEPTION','SQLSTATE','SQLWARNING','START','STATE','STATIC','STATUS','STORAGE','STORE','STORED','STREAM','STRING','STRUCT','STYLE','SUB','SUBMULTISET','SUBPARTITION','SUBSTRING','SUBTYPE','SUM','SUPER','SYMMETRIC','SYNONYM','SYSTEM','TABLE','TABLESAMPLE','TEMP','TEMPORARY','TERMINATED','TEXT','THAN','THEN','THROUGHPUT','TIME','TIMESTAMP','TIMEZONE','TINYINT','TO','TOKEN','TOTAL','TOUCH','TRAILING','TRANSACTION','TRANSFORM','TRANSLATE','TRANSLATION','TREAT','TRIGGER','TRIM','TRUE','TRUNCATE','TTL','TUPLE','TYPE','UNDER','UNDO','UNION','UNIQUE','UNIT','UNKNOWN','UNLOGGED','UNNEST','UNPROCESSED','UNSIGNED','UNTIL','UPDATE','UPPER','URL','USAGE','USE','USER','USERS','USING','UUID','VACUUM','VALUE','VALUED','VALUES','VARCHAR','VARIABLE','VARIANCE','VARINT','VARYING','VIEW','VIEWS','VIRTUAL','VOID','WAIT','WHEN','WHENEVER','WHERE','WHILE','WINDOW','WITH','WITHIN','WITHOUT','WORK','WRAPPED','WRITE','YEAR','ZONE']
# fmt: on


class CompositorTable(object):
    def __init__(
        self,
        table_name: str,
        primary_index: PrimaryIndex,
        attribute_list: List[str],
        secondary_indexes: List[GlobalSecondaryIndex] = None,
        unique_id_attribute_name: str = None,
        stringify_attributes: List[str] = None,
        latest_version_attribute: str = None,
        versioning_attribute: str = None,
        ttl_attribute_name: str = None,
    ):
        self.table_name = table_name
        self.attribute_list = attribute_list
        self.primary_index = primary_index
        self.secondary_indexes = secondary_indexes
        self.unique_id_attribute_name = unique_id_attribute_name
        self.stringify_attributes = stringify_attributes
        self.latest_version_attribute = latest_version_attribute
        self.versioning_attribute = versioning_attribute
        self.versioning_enabled = latest_version_attribute is not None
        self.ttl_attribute = ttl_attribute_name
        self.debug = logger.getEffectiveLevel() <= logging.DEBUG

        self.dynamo_resource = boto3.resource("dynamodb")
        self.dynamo_table = self.dynamo_resource.Table(self.table_name)

    @staticmethod
    def __null_if_empty(value: Union[str, dict, list]):
        return None if isinstance(value, (str, dict, list)) and len(value) < 1 else value

    def get_expression(self, fields: list) -> str:
        fields = fields or self.attribute_list

        projection_expression = []
        expression_attribute_names = {}

        for field in fields:
            if field.upper() in RESERVED_WORDS:
                field_alias = "#"
                while field_alias in expression_attribute_names or field_alias == "#":
                    field_alias = "#" + field[: len(field_alias)]
                expression_attribute_names.update({field_alias: field})
                projection_expression.append(field_alias)
            else:
                projection_expression.append(field)

        response = {"ProjectionExpression": ",".join(projection_expression)}
        if len(expression_attribute_names) > 0:
            response["ExpressionAttributeNames"] = expression_attribute_names

        return response

    def get_next_version(self, field_values: dict, latest_version: int = None) -> int:
        if latest_version is not None:
            return latest_version + 1

        query_args = self.get_expression([self.latest_version_attribute])
        field_values[self.versioning_attribute] = 0
        query_args["Key"] = self.primary_index.full_key(field_values)
        results = self.dynamo_table.get_item(**query_args)
        return int(results.get("Item", {}).get(self.latest_version_attribute, 0)) + 1

    def get_all_key_field_names(self) -> list:
        fields = [self.primary_index.partition_key_name]
        if self.primary_index.sort_key_name:
            fields.append(self.primary_index.sort_key_name)
        for index in self.secondary_indexes:
            if index.partition_key_name:
                fields.append(index.partition_key_name)
            if index.sort_key_name:
                fields.append(index.sort_key_name)
        return list(set(fields))

    def all_item_properties(self) -> list:
        all_field_names = self.attribute_list
        all_field_names += self.primary_index.partition_key_format_fields
        all_field_names += self.primary_index.sort_key_format_fields
        for index in self.secondary_indexes:
            all_field_names += index.partition_key_format_fields
            all_field_names += index.sort_key_format_fields

        return list(set(all_field_names))

    def errant_field_values(self, field_values: dict) -> tuple:
        errant_fields = []
        all_field_names = self.all_item_properties()

        for field in field_values.keys():
            if field not in all_field_names:
                errant_fields.append(field)

        return errant_fields

    def errant_return_fields(self, return_fields: list) -> tuple:
        errant_fields = []
        all_field_names = self.all_item_properties()
        for field in return_fields:
            if field not in all_field_names:
                errant_fields.append(field)

        return errant_fields

    def stringify(self, field_values: dict) -> dict:
        stringify_attributes = self.stringify_attributes or []

        for attribute in stringify_attributes:
            if field_values.get(attribute) is not None and not isinstance(field_values.get(attribute), str):
                field_values[attribute] = json.dumps(field_values[attribute], cls=DdbJsonEncoder)

        return field_values

    def destringify(self, field_values: dict) -> dict:
        stringify_attributes = self.stringify_attributes or []

        for attribute in stringify_attributes:
            if field_values.get(attribute) is not None and field_values.get(attribute) != "":
                field_values[attribute] = json.loads(field_values[attribute])

        return field_values

    def get_query_key_condition_expression(self, field_values: dict, force_key_begins_with: bool = False) -> dict:
        primary_index_score = self.primary_index.query_score(
            field_values=field_values,
            unique_id_attribute_name=self.unique_id_attribute_name,
        )

        secondary_index_scores = []
        for index in self.secondary_indexes:
            secondary_index_scores.append(
                index.query_score(
                    field_values=field_values,
                    unique_id_attribute_name=self.unique_id_attribute_name,
                )
            )

        if all(primary_index_score >= x for x in secondary_index_scores):
            query_args = dict(
                KeyConditionExpression=self.primary_index.get_condition_expression(
                    field_values=field_values,
                    key_score=primary_index_score,
                    force_key_begins_with=force_key_begins_with,
                )
            )
        else:
            secondary_index = self.secondary_indexes[secondary_index_scores.index(max(secondary_index_scores))]
            query_args = dict(
                IndexName=secondary_index.name,
                KeyConditionExpression=secondary_index.get_condition_expression(
                    field_values=field_values,
                    key_score=max(secondary_index_scores),
                    force_key_begins_with=force_key_begins_with,
                ),
            )

        return query_args

    def is_latest(self, item: dict) -> bool:
        if not self.versioning_enabled:
            return False

        field_values = {
            self.versioning_attribute: 0,
            self.primary_index.partition_key_name: item[self.primary_index.partition_key_name],
        }

        for index in self.secondary_indexes:
            field_values[index.partition_key_name] = item[index.partition_key_name]

        primary_index_score = self.primary_index.query_score(field_values=field_values)

        secondary_index_scores = []
        for index in self.secondary_indexes:
            secondary_index_scores.append(index.query_score(field_values=field_values))

        if all(primary_index_score >= x for x in secondary_index_scores):
            sort_best_match = self.primary_index.get_sort_best_match(field_values)
            if item[self.primary_index.sort_key_name].startswith(sort_best_match):
                return True
        else:
            secondary_index = self.secondary_indexes[secondary_index_scores.index(max(secondary_index_scores))]
            sort_best_match = secondary_index.get_sort_best_match(field_values)
            if item[secondary_index.sort_key_name].startswith(sort_best_match):
                return True

        return False

    @staticmethod
    def reverse_format_string(actual_string, format_string):
        fields = list(string.Formatter().parse(format_string))
        field_values = {}

        for idx, field in enumerate(fields):
            actual_string = actual_string[len(field[0]) :]
            if field[1] is not None:
                if (idx + 1) < len(fields):
                    field_values[field[1]] = actual_string.split(fields[idx + 1][0])[0]
                    actual_string = actual_string[len(field_values[field[1]]) :]
                else:
                    field_values[field[1]] = actual_string

        return field_values

    def field_values_from_item_keys(self, item: dict) -> dict:
        field_values = self.reverse_format_string(
            item[self.primary_index.partition_key_name], self.primary_index.partition_key_format
        )
        if self.primary_index.sort_key_name:
            field_values.update(
                self.reverse_format_string(
                    item[self.primary_index.sort_key_name],
                    self.primary_index.sort_key_format,
                )
            )
        for index in self.secondary_indexes:
            field_values.update(self.reverse_format_string(item[index.partition_key_name], index.partition_key_format))

            if index.partition_key_name:
                field_values.update(self.reverse_format_string(item[index.sort_key_name], index.sort_key_format))

        return field_values

    def none_found_response(self):
        return {
            "ResponseMetadata": {"HTTPStatusCode": 404},
            "Items": [],
            "body": {"code": 404, "message": "No item found..."},
        }

    @staticmethod
    def __item_serialize(item: dict) -> dict:
        new_item = {}
        for key in item:
            new_item[key] = TypeSerializer().serialize(item[key])

        return new_item

    @staticmethod
    def __item_deserialize(item: dict) -> dict:
        new_item = {}
        for key in item:
            new_item[key] = TypeDeserializer().deserialize(item[key])

        return new_item

    def __write_items_generator(self, field_values: dict, overwrite: bool = True, latest_version: int = None):
        items = []

        if self.versioning_enabled:
            field_values[self.versioning_attribute] = self.get_next_version(field_values, latest_version)

        item = deepcopy(field_values)
        item.update(self.primary_index.full_key(item))

        for index in self.secondary_indexes:
            item.update(index.full_key(item))

        if not overwrite:
            item.update({"ConditionExpression": self.primary_index.get_ne_conditional(field_values)})

        items.append({"Put": {"TableName": self.table_name, "Item": self.__item_serialize(item)}})

        if self.versioning_enabled:
            item = deepcopy(field_values)
            item[self.versioning_attribute] = 0
            item.update(self.primary_index.full_key(item))

            for index in self.secondary_indexes:
                item.update(index.full_key(item))

            if not overwrite:
                item.update({"ConditionExpression": self.primary_index.get_ne_conditional(field_values)})

            item[self.latest_version_attribute] = item[self.versioning_attribute]

            items.append({"Put": {"TableName": self.table_name, "Item": self.__item_serialize(item)}})

        return items

    def put_item(self, field_values: dict, overwrite: bool = True, latest_version: int = None) -> dict:
        field_values = self.stringify(field_values)
        field_values = self.params_cleanup(field_values)

        if self.versioning_enabled:
            field_values[self.versioning_attribute] = self.get_next_version(field_values, latest_version)

        items = [field_values.copy()]
        items[0].update(self.primary_index.full_key(field_values))
        for index in self.secondary_indexes:
            items[0].update(index.full_key(field_values))

        if self.versioning_enabled:
            items.append(field_values.copy())
            items[1][self.versioning_attribute] = 0
            items[1].update(self.primary_index.full_key(items[1]))
            for index in self.secondary_indexes:
                items[1].update(index.full_key(items[1]))
            items[1][self.latest_version_attribute] = items[1][self.versioning_attribute]

        for item in items:
            put_item_args = {
                "Item": item,
                "ReturnConsumedCapacity": "INDEXES" if self.debug else "NONE",
            }
            if not overwrite:
                put_item_args.update({"ConditionExpression": self.primary_index.get_ne_conditional(field_values)})
            try:
                logger.debug("New item: %s", json.dumps(item, cls=DdbJsonEncoder))
                response = self.dynamo_table.put_item(**put_item_args)
                logger.debug("Put_item: %s", json.dumps(response, cls=DdbJsonEncoder))
                response["ResponseMetadata"]["HTTPStatusCode"] = 201
            except botocore.exceptions.ClientError as e:
                # Ignore the ConditionalCheckFailedException, bubble up
                # other exceptions.
                if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
                    raise
                else:
                    response = e.response
                    response["ResponseMetadata"]["HTTPStatusCode"] = 409
                    response["body"] = {
                        "code": response["ResponseMetadata"]["HTTPStatusCode"],
                        "message": "Duplicate entry found!",
                        "fields": str(self.primary_index.field_values_intersection(field_values)),
                    }
                break

        if "body" not in response:
            response["body"] = self.destringify(field_values)
        return response

    def get_items(
        self,
        field_values: dict,
        return_fields: list,
        force_key_begins_with: bool = False,
        null_if_empty: bool = True,
    ) -> dict:
        field_values = self.params_cleanup(field_values, null_if_empty)

        if self.versioning_enabled:
            field_values[self.versioning_attribute] = field_values.get(self.versioning_attribute, 0)

        query_args = self.get_query_key_condition_expression(field_values, force_key_begins_with)
        query_args.update(self.get_expression(return_fields))
        query_args["ReturnConsumedCapacity"] = "INDEXES" if self.debug else "NONE"

        response = self.dynamo_table.query(**query_args)

        if self.stringify_attributes:
            for item in response["Items"]:
                item = self.destringify(item)

        return response

    def delete_item(self, field_values: dict) -> dict:
        field_values = self.params_cleanup(field_values)
        all_field_names = self.get_all_key_field_names() + self.attribute_list

        query_args = self.get_query_key_condition_expression(field_values)
        query_args.update(self.get_expression(all_field_names))
        query_args["ReturnConsumedCapacity"] = "INDEXES" if self.debug else "NONE"

        existing_items = self.dynamo_table.query(**query_args)

        if len(existing_items["Items"]) < 1:
            return self.none_found_response()

        del_item = None
        delete_items = []

        for item in existing_items["Items"]:
            item = self.destringify(item)
            if (not self.versioning_enabled and del_item is None) or self.is_latest(item):
                del_item = item.copy()

            logger.debug(
                "Deleting item: %s:%s",
                k=item[self.primary_index.partition_key_name],
                s=item.get(
                    self.primary_index.sort_key_name,
                    "",
                ),
            )

            delete_item = {
                "DeleteRequest": {
                    "Key": {self.primary_index.partition_key_name: item[self.primary_index.partition_key_name]}
                }
            }

            if self.primary_index.sort_key_name:
                attrib_name = self.primary_index.sort_key_name
                delete_item["DeleteRequest"]["Key"][attrib_name] = item[attrib_name]

            delete_items.append(delete_item)

        response = self.dynamo_resource.batch_write_item(RequestItems={self.table_name: delete_items})

        logger.debug("DynamoDB 'delete_item' response: %s", json.dumps(response, cls=DdbJsonEncoder))

        # Cleanup del_item
        for field_name in self.get_all_key_field_names():
            del del_item[field_name]

        response["body"] = {"item": del_item, "deleted": True}

        return response

    def update_item(self, field_values: dict, force_overwrite: bool = False) -> dict:
        field_values = self.stringify(field_values)
        field_values = self.params_cleanup(field_values)
        latest_item_values = field_values.copy()
        return_fields = self.get_all_key_field_names()

        if self.versioning_enabled and not force_overwrite:
            latest_item_values[self.versioning_attribute] = 0
            return_fields.append(self.latest_version_attribute)
        latest_item = self.get_items(latest_item_values, return_fields)

        if len(latest_item["Items"]) < 1:
            return self.none_found_response()

        latest_item = latest_item["Items"][0]

        field_values.update(self.field_values_from_item_keys(latest_item))

        latest_version = None
        if self.versioning_enabled and not force_overwrite:
            latest_version = latest_item.pop(self.latest_version_attribute)

        return self.put_item(field_values=field_values, latest_version=latest_version)

    @staticmethod
    def params_cleanup(params: dict, null_if_empty: bool = True) -> dict:
        for key, value in params.items():
            if isinstance(value, str) and value is not None:
                value = value.strip()
            elif isinstance(value, dict):
                params[key] = __class__.params_cleanup(value)

            if null_if_empty:
                params[key] = CompositorTable.__null_if_empty(value)
        return params
