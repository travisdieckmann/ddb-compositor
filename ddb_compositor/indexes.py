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

import logging
from os import environ

from ddb_compositor.base_index import IndexType, Index

logger = logging.getLogger(__name__)
logger.setLevel(environ.get("LOG_LEVEL", logging.INFO))


class PrimaryIndex(Index):
    def __init__(
        self,
        hash_key_name: str,
        hash_key_format: str,
        range_key_name: str = None,
        range_key_format: str = None,
        composite_separator: str = None,
    ):
        super().__init__(
            hash_key_name,
            hash_key_format,
            range_key_name,
            range_key_format,
            index_type=IndexType.PRIMARY,
            name=None,
            composite_separator=composite_separator,
        )


class GlobalSecondaryIndex(Index):
    def __init__(
        self,
        name: str,
        hash_key_name: str,
        hash_key_format: str,
        range_key_name: str = None,
        range_key_format: str = None,
        composite_separator: str = None,
    ):
        super().__init__(
            hash_key_name,
            hash_key_format,
            range_key_name,
            range_key_format,
            name=name,
            index_type=IndexType.GLOBAL_SECONDARY,
            composite_separator=composite_separator,
        )


class LocalSecondaryIndex(Index):
    """[summary]

    Arguments:
        Index {[type]} -- [description]
    """

    def __init__(
        self,
        name: str,
        hash_key_name: str,
        hash_key_format: str,
        range_key_name: str = None,
        range_key_format: str = None,
        composite_separator: str = None,
    ):
        super().__init__(
            hash_key_name,
            hash_key_format,
            range_key_name,
            range_key_format,
            name=name,
            index_type=IndexType.LOCAL_SECONDARY,
            composite_separator=composite_separator,
        )
