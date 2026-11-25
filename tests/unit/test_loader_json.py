from decimal import Decimal

import pytest

from migration_pipeline.loaders.json_values import encode_json, equivalent_json


def test_json_keeps_decimal_precision_and_distinguishes_booleans() -> None:
    encoded = encode_json({"numbers": [Decimal("1234567890.123456789012345678901"), True, None]})
    assert encoded == '{"numbers":[1234567890.123456789012345678901,true,null]}'
    assert equivalent_json('{"n": [1.000, true]}', '{"n":[1,true]}')
    assert not equivalent_json('{"n":true}', '{"n":1}')


def test_json_rejects_non_finite_decimal() -> None:
    with pytest.raises(ValueError, match="должно быть конечным"):
        encode_json(Decimal("NaN"))
