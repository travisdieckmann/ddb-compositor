from botocore.vendored.six import assertRaisesRegex
import pytest
from decimal import Decimal
from json import JSONEncoder
from datetime import datetime, date

from ddb_compositor.utility import DdbJsonEncoder


def test_ddb_json_encoder():
    assert isinstance(DdbJsonEncoder(), JSONEncoder)
    assert "default" in dir(DdbJsonEncoder())

    assert isinstance(DdbJsonEncoder().default(Decimal("2.03")), float)
    assert DdbJsonEncoder().default(Decimal("2.03")) == 2.03

    assert isinstance(DdbJsonEncoder().default(datetime.now()), str)
    assert DdbJsonEncoder().default(datetime.fromisoformat("2021-05-16 14:40:06")) == "2021-05-16 14:40:06"

    assert isinstance(DdbJsonEncoder().default(date.today()), str)
    assert DdbJsonEncoder().default(date.fromisoformat("2021-05-16")) == "2021-05-16"

    with pytest.raises(TypeError):
        DdbJsonEncoder().default(42)
