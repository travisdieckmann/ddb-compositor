import pytest
from ddb_compositor.base_index import Index, IndexType

primary_index = Index(
    partition_key_name="some_index",
    partition_key_format="aslr:{some_val}:{some_val2}",
    index_type=IndexType.PRIMARY,
    name="PrimaryIndex",
    composite_separator=":",
)

primary_index_with_sort = Index(
    partition_key_name="some_index",
    partition_key_format="aslr:{some_val}:{some_val2}",
    sort_key_name="some_index_range",
    sort_key_format="dslr:{some_new_val}:{some_new_val2}",
    index_type=IndexType.PRIMARY,
    name="PrimaryIndex",
    composite_separator=":",
)


def test_index_init():
    assert primary_index_with_sort.is_primary
    assert primary_index_with_sort.name == "PrimaryIndex"
    assert primary_index_with_sort.type == IndexType.PRIMARY
    assert primary_index_with_sort.partition_key_format == "aslr:{some_val}:{some_val2}"


def test_index_partition_key_value():
    partition_key_value = primary_index.partition_key_value(field_values={"some_val": "12345", "some_val2": "abc"})

    assert partition_key_value == {"some_index": "aslr:12345:abc"}


def test_index_sort_key_value():
    assert primary_index.sort_key_value({"some_new_val": "12345", "some_new_val2": "abc"}) == {}
    assert primary_index_with_sort.sort_key_value({"some_new_val": "12345", "some_new_val2": "abc"}) == {
        "some_index_range": "dslr:12345:abc"
    }


def test_index_full_key():
    full_key = primary_index.full_key({"some_val": "12345", "some_val2": "abc", "some_new_val": "67890"})

    assert full_key == {"some_index": "aslr:12345:abc"}

    full_key = primary_index_with_sort.full_key(
        {
            "some_val": "12345",
            "some_val2": "abc",
            "some_new_val": "67890",
            "some_new_val2": "def",
        }
    )

    assert full_key == {
        "some_index": "aslr:12345:abc",
        "some_index_range": "dslr:67890:def",
    }


def test_index_get_ne_conditional():
    index_conditional = primary_index.get_ne_conditional(
        {
            "some_val": "12345",
            "some_val2": "abc",
            "some_new_val": "67890",
            "some_new_val2": "def",
        }
    )
    assert index_conditional.get_expression()["operator"] == "<>"
    assert index_conditional.get_expression()["values"][0].name == "some_index"
    assert index_conditional.get_expression()["values"][1] == "aslr:12345:abc"

    index_conditional = primary_index_with_sort.get_ne_conditional(
        {
            "some_val": "12345",
            "some_val2": "abc",
            "some_new_val": "67890",
            "some_new_val2": "def",
        }
    )
    assert index_conditional.get_expression()["operator"] == "AND"

    condition = index_conditional.get_expression()["values"][0]
    assert condition.get_expression()["operator"] == "<>"
    assert condition.get_expression()["values"][0].name == "some_index"
    assert condition.get_expression()["values"][1] == "aslr:12345:abc"

    condition = index_conditional.get_expression()["values"][1]
    assert condition.get_expression()["operator"] == "<>"
    assert condition.get_expression()["values"][0].name == "some_index_range"
    assert condition.get_expression()["values"][1] == "dslr:67890:def"


def test_format_string_field_list():
    assert "some_val" in primary_index_with_sort.partition_key_format_fields
    assert "some_new_val" in primary_index_with_sort.sort_key_format_fields


def test_partition_key_ordered_intersection():
    field_values = {
        "some_val": "12345",
        "some_val2": "abc",
        "some_new_val": "67890",
        "some_new_val2": "def",
    }

    assert primary_index_with_sort.partition_key_ordered_intersection(field_values)[0] == "some_val"
    assert primary_index_with_sort.partition_key_ordered_intersection(field_values)[1] == "some_val2"


def test_sort_key_ordered_intersection():
    field_values = {
        "some_val": "12345",
        "some_val2": "abc",
        "some_new_val": "67890",
        "some_new_val2": "def",
    }
    assert primary_index_with_sort.sort_key_ordered_intersection(field_values)[0] == "some_new_val"
    assert primary_index_with_sort.sort_key_ordered_intersection(field_values)[1] == "some_new_val2"


def test_field_values_intersection():
    common_fields = primary_index_with_sort.field_values_intersection({"some_val": "yes"})
    assert "some_val" in common_fields.keys()

    common_fields = primary_index_with_sort.field_values_intersection({"some_new_val": "yes"})
    assert "some_new_val" in common_fields.keys()

    common_fields = primary_index_with_sort.field_values_intersection({"some_val": "yes", "some_new_val": "no"})
    assert "some_val" in common_fields.keys()
    assert "some_new_val" in common_fields.keys()


def test_query_score():
    index = Index(
        partition_key_name="some_index",
        partition_key_format="aslr:{some_val}:{some_val2}",
        sort_key_name="some_index_range",
        sort_key_format="dslr:{some_new_val}:{some_new_val2}",
        index_type=IndexType.PRIMARY,
        name="PrimaryIndex",
        composite_separator=":",
    )

    score = index.query_score(field_values={"some_val": "abcd", "some_val2": "1234"})
    assert score == 0

    score = index.query_score(field_values={"some_val": "abcd", "some_val2": "1234", "some_new_val": "efgh"})
    assert score == 50

    score = index.query_score(
        field_values={
            "some_val": "abcd",
            "some_val2": "1234",
            "some_new_val": "efgh",
            "some_new_val2": "5678",
        }
    )
    assert score == 100

    score = index.query_score(
        field_values={
            "some_val": "abcd",
            "some_new_val": "efgh",
            "some_new_val2": "5678",
        }
    )
    assert score == 0

    score = index.query_score(
        field_values={
            "some_val": "abcd",
            "some_val2": "1234",
            "some_new_val2": "5678",
        }
    )
    assert score == 0


def test_get_condition_expression():
    index = Index(
        partition_key_name="some_index",
        partition_key_format="aslr:{some_val}:{some_val2}",
        index_type=IndexType.PRIMARY,
        name="PrimaryIndex",
        composite_separator=":",
    )

    condition_expression = index.get_condition_expression(
        field_values={"some_val": "abcd", "some_val2": "1234", "some_new_val": "efgh"},
        key_score=100,
    )

    assert condition_expression.get_expression()["values"][1] == "aslr:abcd:1234"
