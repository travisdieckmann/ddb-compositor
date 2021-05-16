import json
import inspect
import pytest
import os
import botocore
from io import StringIO
from mock import patch, call
import re
from ddb_compositor import CompositorTable, PrimaryIndex, GlobalSecondaryIndex


test_table = CompositorTable(
    table_name="test_table",
    attribute_list=[
        "tenant_id",
        "id",
        "name",
        "type",
        "flowFilterId",
        "description",
        "dataTypes",
        "dataPoints",
        "version",
        "createdAt",
        "createdBy",
    ],
    primary_index=PrimaryIndex(
        partition_key_name="pk",
        partition_key_format="{tenant_id}",
        sort_key_name="ps",
        sort_key_format="datadefinition:v{version}:{flowFilterId}:{id}",
        composite_separator=":",
    ),
    secondary_indexes=[
        GlobalSecondaryIndex(
            name="GSI",
            partition_key_name="pk",
            partition_key_format="{tenant_id}",
            sort_key_name="gss",
            sort_key_format="datadefinition:{id}:v{version}",
            composite_separator=":",
        )
    ],
    unique_id_attribute_name="id",
    latest_version_attribute="latest",
    versioning_attribute="version",
    ttl_attribute_name="ttl",
)


@patch("botocore.client.BaseClient._make_api_call")
def test_create_item_request(boto_mock):
    # The mocked DynamoDB response.
    expected_ddb_response = {"ResponseMetadata": {"HTTPStatusCode": 201}}
    # The mocked response we expect back by calling DynamoDB through boto.
    response_body = botocore.response.StreamingBody(
        StringIO(str(expected_ddb_response)), len(str(expected_ddb_response))
    )
    # Setting the expected value in the mock.
    boto_mock.side_effect = [
        expected_ddb_response,
        expected_ddb_response,
        expected_ddb_response,
    ]
    # Expecting that there would be a call to DynamoDB Scan function during execution with these parameters.

    # Call the function to test.
    result = test_table.put_item(
        field_values={
            "tenant_id": "000000000001",
            "id": "0123456789ABCDEF",
            "name": "Hot Garbage",
            "type": "database",
            "description": "Primary Microsoft SQL Server 2016",
            "flowFilterId": "asdfkljbnebab",
            "dataTypes": {
                "Table": {
                    "childTypes": ["String", "Number"],
                    "description": "HL7 Composite",
                    "isArray": True,
                },
                "Number": {
                    "childTypes": [],
                    "description": "Number value data field",
                    "isArray": False,
                },
                "String": {
                    "childTypes": [],
                    "description": "String value data field",
                    "isArray": False,
                },
            },
            "dataPoints": {
                "55B3E25800031383B40E4CE0729AB4D7": {
                    "alias": "CollectorÂs Comment",
                    "dataType": "Composite",
                    "enc": False,
                    "parentUuid": "55B3E258B802669A9809615BB7E3590F",
                    "req": False,
                    "rpt": 0,
                    "index": 38,
                    "mask": False,
                    "risk_level": 0,
                }
            },
        },
        overwrite=False,
    )

    # Run unit test assertions to verify the expected calls to mock have occurred and verify the response.

    assert result["ResponseMetadata"]["HTTPStatusCode"] == 201

    result_body = result.get("body")
    assert result_body["name"] == "Hot Garbage"

    assert boto_mock.call_count == 3

    call1 = boto_mock.mock_calls[1][1][1]
    assert call1["TableName"] == "test_table"
    assert call1["Item"]["pk"] == "000000000001"
    assert re.match(r"^datadefinition:([A-Za-z0-9]*):v1$", call1["Item"]["gss"])
    assert call1["Item"]["dataTypes"]["Table"]["description"] == "HL7 Composite"
    assert call1["Item"]["ps"].startswith("datadefinition:v1:asdfkljbnebab")

    condition_expression_values = call1["ConditionExpression"].get_expression()["values"]
    assert condition_expression_values[0].get_expression()["values"][1] == "000000000001"
    assert condition_expression_values[1].get_expression()["values"][1].startswith("datadefinition:v1:asdfkljbnebab")

    call2 = boto_mock.mock_calls[2][1][1]
    assert call2["TableName"] == "test_table"
    assert call2["Item"]["pk"] == "000000000001"
    assert re.match(r"^datadefinition:([A-Za-z0-9]*):v0$", call2["Item"]["gss"])
    assert call2["Item"]["dataPoints"]["55B3E25800031383B40E4CE0729AB4D7"]["dataType"] == "Composite"
    assert call2["Item"]["ps"].startswith("datadefinition:v0:asdfkljbnebab")


@patch("botocore.client.BaseClient._make_api_call")
def test_delete_item_request(boto_mock):
    # The mocked values in our DynamoDB table.
    items_in_db = [
        {
            "createdAt": "1558058448201",
            "dataPoints": {
                "55B3E25800031383B40E4CE0729AB4D7": {
                    "alias": "CollectorÂs Comment",
                    "dataType": "Composite",
                    "enc": False,
                    "index": 38,
                    "mask": False,
                    "parentUuid": "55B3E258B802669A9809615BB7E3590F",
                    "req": False,
                    "risk_level": 0,
                    "rpt": 0,
                }
            },
            "dataTypes": {
                "Number": {
                    "childTypes": [],
                    "description": "Number value data field",
                    "isArray": False,
                },
                "String": {
                    "childTypes": [],
                    "description": "String value data field",
                    "isArray": False,
                },
                "Table": {
                    "childTypes": ["String", "Number"],
                    "description": "HL7 Composite",
                    "isArray": True,
                },
            },
            "description": "Primary Microsoft SQL Server 2016",
            "gss": "datadefinition:H6QMWX7YM9VfpQJj3Ds3Kf:v0",
            "tenant_id": "000000000001",
            "id": "H6QMWX7YM9VfpQJj3Ds3Kf",
            "flowFilterIt": "asdfkljbnebab",
            "latest": 1,
            "version": 1,
            "name": "Hot Garbage",
            "pk": "000000000001",
            "ps": "datadefinition:v0:asdfkljbnebab:H6QMWX7YM9VfpQJj3Ds3Kf",
        },
        {
            "createdAt": "1558058448201",
            "dataPoints": {
                "55B3E25800031383B40E4CE0729AB4D7": {
                    "alias": "CollectorÂs Comment",
                    "dataType": "Composite",
                    "enc": False,
                    "index": 38,
                    "mask": False,
                    "parentUuid": "55B3E258B802669A9809615BB7E3590F",
                    "req": False,
                    "risk_level": 0,
                    "rpt": 0,
                }
            },
            "dataTypes": {
                "Number": {
                    "childTypes": [],
                    "description": "Number value data field",
                    "isArray": False,
                },
                "String": {
                    "childTypes": [],
                    "description": "String value data field",
                    "isArray": False,
                },
                "Table": {
                    "childTypes": ["String", "Number"],
                    "description": "HL7 Composite",
                    "isArray": True,
                },
            },
            "description": "Primary Microsoft SQL Server 2016",
            "gss": "datadefinition:H6QMWX7YM9VfpQJj3Ds3Kf:v1",
            "tenant_id": "000000000001",
            "id": "H6QMWX7YM9VfpQJj3Ds3Kf",
            "flowFilterIt": "asdfkljbnebab",
            "version": 1,
            "name": "Hot Garbage",
            "type": "database",
            "pk": "000000000001",
            "ps": "datadefinition:v1:asdfkljbnebab:H6QMWX7YM9VfpQJj3Ds3Kf",
        },
    ]
    # The mocked DynamoDB response.
    expected_ddb_response = {
        "Items": items_in_db,
        "ResponseMetadata": {"HTTPStatusCode": 200},
        "UnprocessedItems": {},
    }
    # The mocked response we expect back by calling DynamoDB through boto.
    response_body = botocore.response.StreamingBody(
        StringIO(str(expected_ddb_response)), len(str(expected_ddb_response))
    )
    # Setting the expected value in the mock.
    boto_mock.side_effect = [expected_ddb_response, expected_ddb_response]
    # Expecting that there would be a call to DynamoDB Scan function during execution with these parameters.

    # Call the function to test.
    result = test_table.delete_item(field_values={"tenant_id": "000000000001", "id": "H6QMWX7YM9VfpQJj3Ds3Kf"})

    # Run unit test assertions to verify the expected calls to mock have occurred and verify the response.
    assert result["ResponseMetadata"]["HTTPStatusCode"] == 200

    result_body = result.get("body")
    assert result_body["item"]["name"] == "Hot Garbage"
    assert result_body["deleted"] == True

    assert boto_mock.call_count == 2

    tenant_id = "000000000001"

    call1 = boto_mock.mock_calls[0][1][1]
    assert call1["TableName"] == "test_table"
    assert call1["IndexName"] == "GSI"

    key_conditions = call1["KeyConditionExpression"].get_expression()["values"]
    assert key_conditions[0].get_expression()["values"][1] == tenant_id
    assert key_conditions[1].get_expression()["values"][1] == "datadefinition:H6QMWX7YM9VfpQJj3Ds3Kf:v"

    call2 = boto_mock.mock_calls[1][1][1]
    assert "test_table" in call2["RequestItems"]

    batch_items = call2["RequestItems"]["test_table"]
    item = batch_items[0]["DeleteRequest"]["Key"]
    assert item["pk"] == tenant_id
    assert item["ps"] == "datadefinition:v0:asdfkljbnebab:H6QMWX7YM9VfpQJj3Ds3Kf"

    item = batch_items[1]["DeleteRequest"]["Key"]
    assert item["pk"] == tenant_id
    assert item["ps"] == "datadefinition:v1:asdfkljbnebab:H6QMWX7YM9VfpQJj3Ds3Kf"


@patch("botocore.client.BaseClient._make_api_call")
def test_item_list_success(boto_mock):
    # The mocked values in our DynamoDB table.
    items_in_db = [
        {
            "createdAt": "1558058448201",
            "dataPoints": {
                "55B3E25800031383B40E4CE0729AB4D7": {
                    "alias": "CollectorÂs Comment",
                    "dataType": "Composite",
                    "enc": False,
                    "index": 38,
                    "mask": False,
                    "parentUuid": "55B3E258B802669A9809615BB7E3590F",
                    "req": False,
                    "risk_level": 0,
                    "rpt": 0,
                }
            },
            "dataTypes": {
                "Number": {
                    "childTypes": [],
                    "description": "Number value data field",
                    "isArray": False,
                },
                "String": {
                    "childTypes": [],
                    "description": "String value data field",
                    "isArray": False,
                },
                "Table": {
                    "childTypes": ["String", "Number"],
                    "description": "HL7 Composite",
                    "isArray": True,
                },
            },
            "description": "Primary Microsoft SQL Server 2016",
            "gss": "datadefinition:H6QMWX7YM9VfpQJj3Ds3Kf:v0",
            "tenant_id": "000000000001",
            "id": "H6QMWX7YM9VfpQJj3Ds3Kf",
            "flowFilterId": "asdfkljbnebab",
            "latest": 1,
            "version": 1,
            "name": "Hot Garbage",
            "type": "database",
            "pk": "000000000001",
            "ps": "datadefinition:v0:asdfkljbnebab:H6QMWX7YM9VfpQJj3Ds3Kf",
        }
    ]
    # The mocked DynamoDB response.
    expected_ddb_response = {
        "Items": items_in_db,
        "ResponseMetadata": {"HTTPStatusCode": 200},
        "UnprocessedItems": {},
    }
    # The mocked response we expect back by calling DynamoDB through boto.
    response_body = botocore.response.StreamingBody(
        StringIO(str(expected_ddb_response)), len(str(expected_ddb_response))
    )
    # Setting the expected value in the mock.
    boto_mock.side_effect = [expected_ddb_response]

    # Call the function to test.
    query_params = {"tenant_id": "000000000001"}
    return_fields = [
        "id",
        "name",
        "description",
        "type",
        "flowFilterId",
        "version",
        "createdAt",
    ]
    result = test_table.get_items(field_values=query_params, return_fields=return_fields)

    # Run unit test assertions to verify the expected calls to mock have occurred and verify the response.
    # assert result.get('headers').get('Content-Type') == 'application/json'
    assert result["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert boto_mock.call_count == 1

    tenant_id = "000000000001"

    call1 = boto_mock.mock_calls[0][1][1]
    assert call1["TableName"] == "test_table"

    key_conditions = call1["KeyConditionExpression"].get_expression()["values"]
    assert key_conditions[0].get_expression()["values"][1] == tenant_id
    assert key_conditions[1].get_expression()["values"][1] == "datadefinition:v0:"


@patch("botocore.client.BaseClient._make_api_call")
def test_item_read_success(boto_mock):
    # The mocked values in our DynamoDB table.
    items_in_db = [
        {
            "createdAt": "1558058448201",
            "dataPoints": {
                "55B3E25800031383B40E4CE0729AB4D7": {
                    "alias": "CollectorÂs Comment",
                    "dataType": "Composite",
                    "enc": False,
                    "index": 38,
                    "mask": False,
                    "parentUuid": "55B3E258B802669A9809615BB7E3590F",
                    "req": False,
                    "risk_level": 0,
                    "rpt": 0,
                }
            },
            "dataTypes": {
                "Number": {
                    "childTypes": [],
                    "description": "Number value data field",
                    "isArray": False,
                },
                "String": {
                    "childTypes": [],
                    "description": "String value data field",
                    "isArray": False,
                },
                "Table": {
                    "childTypes": ["String", "Number"],
                    "description": "HL7 Composite",
                    "isArray": True,
                },
            },
            "description": "Primary Microsoft SQL Server 2016",
            "gss": "datadefinition:H6QMWX7YM9VfpQJj3Ds3Kf:v0",
            "tenant_id": "000000000001",
            "id": "H6QMWX7YM9VfpQJj3Ds3Kf",
            "flowFilterId": "asdfkljbnebab",
            "latest": 1,
            "version": 1,
            "name": "Hot Garbage",
            "type": "database",
            "pk": "000000000001",
            "ps": "datadefinition:v0:asdfkljbnebab:H6QMWX7YM9VfpQJj3Ds3Kf",
        }
    ]
    # The mocked DynamoDB response.
    expected_ddb_response = {
        "Items": items_in_db,
        "ResponseMetadata": {"HTTPStatusCode": 200},
        "UnprocessedItems": {},
    }
    # The mocked response we expect back by calling DynamoDB through boto.
    response_body = botocore.response.StreamingBody(
        StringIO(str(expected_ddb_response)), len(str(expected_ddb_response))
    )
    # Setting the expected value in the mock.
    boto_mock.side_effect = [expected_ddb_response, expected_ddb_response]
    # Expecting that there would be a call to DynamoDB Scan function during execution with these parameters.

    # Call the function to test.
    query_params = {"tenant_id": "000000000001", "id": "H6QMWX7YM9VfpQJj3Ds3Kf"}
    return_fields = None
    result = test_table.get_items(field_values=query_params, return_fields=return_fields)

    # Run unit test assertions to verify the expected calls to mock have occurred and verify the response.
    assert result["ResponseMetadata"]["HTTPStatusCode"] == 200

    result_body = result.get("Items")[0]
    assert result_body["name"] == "Hot Garbage"
    assert result_body["id"] == "H6QMWX7YM9VfpQJj3Ds3Kf"

    assert boto_mock.call_count == 1

    tenant_id = "000000000001"

    call1 = boto_mock.mock_calls[0][1][1]
    assert call1["TableName"] == "test_table"
    assert call1["IndexName"] == "GSI"

    key_conditions = call1["KeyConditionExpression"].get_expression()["values"]
    assert key_conditions[0].get_expression()["values"][1] == tenant_id
    assert key_conditions[1].get_expression()["values"][1] == "datadefinition:H6QMWX7YM9VfpQJj3Ds3Kf:v0"


@patch("botocore.client.BaseClient._make_api_call")
def test_datadefinition_update_success(boto_mock):
    # The mocked values in our DynamoDB table.
    items_in_db = [
        {
            "createdAt": "1558058448201",
            "dataPoints": {
                "55B3E25800031383B40E4CE0729AB4D7": {
                    "alias": "CollectorÂs Comment",
                    "dataType": "Composite",
                    "enc": False,
                    "index": 38,
                    "mask": False,
                    "parentUuid": "55B3E258B802669A9809615BB7E3590F",
                    "req": False,
                    "risk_level": 0,
                    "rpt": 0,
                }
            },
            "dataTypes": {
                "Number": {
                    "childTypes": [],
                    "description": "Number value data field",
                    "isArray": False,
                },
                "String": {
                    "childTypes": [],
                    "description": "String value data field",
                    "isArray": False,
                },
                "Table": {
                    "childTypes": ["String", "Number"],
                    "description": "HL7 Composite",
                    "isArray": True,
                },
            },
            "description": "Primary Microsoft SQL Server 2016",
            "gss": "datadefinition:H6QMWX7YM9VfpQJj3Ds3Kf:v0",
            "tenant_id": "000000000001",
            "id": "H6QMWX7YM9VfpQJj3Ds3Kf",
            "flowFilterId": "asdfkljbnebab",
            "latest": 1,
            "version": 1,
            "name": "Hot Garbage",
            "type": "database",
            "pk": "000000000001",
            "ps": "datadefinition:v0:asdfkljbnebab:H6QMWX7YM9VfpQJj3Ds3Kf",
        }
    ]
    # The mocked DynamoDB response.
    expected_ddb_response = {
        "Items": items_in_db,
        "ResponseMetadata": {"HTTPStatusCode": 200},
        "UnprocessedItems": {},
    }
    # The mocked response we expect back by calling DynamoDB through boto.
    response_body = botocore.response.StreamingBody(
        StringIO(str(expected_ddb_response)), len(str(expected_ddb_response))
    )
    # Setting the expected value in the mock.
    boto_mock.side_effect = [
        expected_ddb_response,
        expected_ddb_response,
        expected_ddb_response,
    ]
    # Expecting that there would be a call to DynamoDB Scan function during execution with these parameters.

    body = dict(items_in_db[0])
    # Call the function to test.
    body["tenant_id"] = "000000000001"
    body["id"] = "H6QMWX7YM9VfpQJj3Ds3Kf"
    body["createdBy"] = "george"

    result = test_table.update_item(field_values=body)

    # Run unit test assertions to verify the expected calls to mock have occurred and verify the response.
    assert result["ResponseMetadata"]["HTTPStatusCode"] == 201

    result_body = result.get("body")
    assert result_body["name"] == "Hot Garbage"

    assert boto_mock.call_count == 3

    tenant_id = "000000000001"

    call1 = boto_mock.mock_calls[0][1][1]
    assert call1["TableName"] == "test_table"
    assert call1["IndexName"] == "GSI"

    key_conditions = call1["KeyConditionExpression"].get_expression()["values"]
    assert key_conditions[0].get_expression()["values"][1] == tenant_id
    assert key_conditions[1].get_expression()["values"][1] == items_in_db[0]["gss"]

    call2 = boto_mock.mock_calls[1][1][1]
    assert call2["TableName"] == "test_table"
    assert call2["Item"]["pk"] == tenant_id
    assert re.match(r"^datadefinition:([A-Za-z0-9]*):v2$", call2["Item"]["gss"])
    assert call2["Item"]["dataTypes"]["Table"]["description"] == "HL7 Composite"
    assert call2["Item"]["ps"].startswith("datadefinition:v2:asdfkljbnebab")

    call3 = boto_mock.mock_calls[2][1][1]
    assert call3["TableName"] == "test_table"
    assert call3["Item"]["pk"] == tenant_id
    assert re.match(r"^datadefinition:([A-Za-z0-9]*):v0$", call3["Item"]["gss"])
    assert call3["Item"]["dataPoints"]["55B3E25800031383B40E4CE0729AB4D7"]["dataType"] == "Composite"
    assert call3["Item"]["ps"].startswith("datadefinition:v0:asdfkljbnebab")


no_range_test_table = CompositorTable(
    table_name="no_range_test_table",
    attribute_list=[
        "uid",
        "id",
        "tenant_id",
        "endpoint_id",
        "type",
        "name",
        "description",
        "params",
        "createdAt",
        "createdBy",
    ],
    primary_index=PrimaryIndex(
        partition_key_name="pk",
        partition_key_format="procEndpoint:{uid}",
        composite_separator=":",
    ),
    secondary_indexes=[],
    unique_id_attribute_name="uid",
)


@patch("botocore.client.BaseClient._make_api_call")
def test_no_sort_item_read_success(boto_mock):
    # The mocked values in our DynamoDB table.
    items_in_db = [
        {
            "active": True,
            "createdAt": "1596295911171",
            "createdBy": "travis@concerted.io",
            "dataDirection": "Source",
            "description": "The best RESTful",
            "endpointType": "rest|unauthenticated",
            "externalIdentity": False,
            "id": "BbXQCqsCgSSFDeEvexezU9",
            "name": "Test RESTful",
            "params": {
                "delegation": None,
                "path_params": [{"path": "part"}],
                "queryParams": {"something": "good"},
                "uid": "2s5dPepNhcXIY5hGVNSDlW6H2DVQd3lrvPME9Vgh9M7z",
                "url": "https://https://engine-dev.concerted.io/v1/services/2s5dPepNhcXIY5hGVNSDlW6H2DVQd3lrvPME9Vgh9M7z",
            },
            "pk": "procEndpoint:2s5dPepNhcXIY5hGVNSDlW6H2DVQd3lrvPME9Vgh9M7z",
            "status": "Pending",
            "tenant_id": "000000000001",
            "uid": "2s5dPepNhcXIY5hGVNSDlW6H2DVQd3lrvPME9Vgh9M7z",
            "version": 1,
        }
    ]
    # The mocked DynamoDB response.
    expected_ddb_response = {
        "Items": items_in_db,
        "ResponseMetadata": {"HTTPStatusCode": 200},
        "UnprocessedItems": {},
    }
    # The mocked response we expect back by calling DynamoDB through boto.
    response_body = botocore.response.StreamingBody(
        StringIO(str(expected_ddb_response)), len(str(expected_ddb_response))
    )
    # Setting the expected value in the mock.
    boto_mock.side_effect = [expected_ddb_response, expected_ddb_response]
    # Expecting that there would be a call to DynamoDB Scan function during execution with these parameters.

    # Call the function to test.
    query_params = {"uid": "2s5dPepNhcXIY5hGVNSDlW6H2DVQd3lrvPME9Vgh9M7z"}
    return_fields = None
    result = no_range_test_table.get_items(field_values=query_params, return_fields=return_fields)

    # Run unit test assertions to verify the expected calls to mock have occurred and verify the response.
    assert result["ResponseMetadata"]["HTTPStatusCode"] == 200

    result_body = result.get("Items")[0]
    assert result_body["name"] == "Test RESTful"
    assert result_body["uid"] == "2s5dPepNhcXIY5hGVNSDlW6H2DVQd3lrvPME9Vgh9M7z"

    assert boto_mock.call_count == 1

    tenant_id = "000000000001"

    call1 = boto_mock.mock_calls[0][1][1]
    assert call1["TableName"] == "no_range_test_table"
    # assert call1["IndexName"] == "GSI"

    key_conditions = call1["KeyConditionExpression"].get_expression()["values"]
    assert key_conditions[0].name == "pk"
    assert key_conditions[1] == "procEndpoint:2s5dPepNhcXIY5hGVNSDlW6H2DVQd3lrvPME9Vgh9M7z"
