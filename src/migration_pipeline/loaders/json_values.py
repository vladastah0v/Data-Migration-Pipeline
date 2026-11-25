import json
from decimal import Decimal


def encode_json(value: object) -> str:
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("Число JSON должно быть конечным.")
        return str(value)
    if isinstance(value, dict):
        return (
            "{"
            + ",".join(f"{json.dumps(key)}:{encode_json(item)}" for key, item in value.items())
            + "}"
        )
    if isinstance(value, list):
        return "[" + ",".join(encode_json(item) for item in value) + "]"
    return json.dumps(value, allow_nan=False, separators=(",", ":"))


def equivalent_json(left: str, right: str) -> bool:
    return _equivalent(
        json.loads(left, parse_float=Decimal), json.loads(right, parse_float=Decimal)
    )


def _equivalent(left: object, right: object) -> bool:
    if isinstance(left, bool) or isinstance(right, bool):
        return type(left) is type(right) and left == right
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(
            _equivalent(left[key], right[key]) for key in left
        )
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(
            _equivalent(a, b) for a, b in zip(left, right, strict=True)
        )
    return left == right
