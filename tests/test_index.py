import pytest

from ddb_compositor.indexes import PrimaryIndex, GlobalSecondaryIndex, LocalSecondaryIndex


def test_primary_index():
    assert PrimaryIndex(partition_key_name="a", partition_key_format="stuff").is_primary


def test_global_secondary_index():
    assert GlobalSecondaryIndex(name="gsi1", partition_key_name="a", partition_key_format="stuff").is_global_secondary


def test_local_secondary_index():
    assert LocalSecondaryIndex(name="lsi1", partition_key_name="a", partition_key_format="stuff").is_local_secondary
