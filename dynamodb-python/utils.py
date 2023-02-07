from decimal import Decimal
from itertools import islice
from typing import Any, Iterable


def normalize_dynamodb_write(obj: Any) -> Any:
    """All dictionary keys need to be strings in DynamoDB."""

    def normalize_values(obj: Any) -> Any:
        """DynamoDB does not support float, change to Decimal instead."""
        if isinstance(obj, float):
            return Decimal(str(obj))
        return obj

    if isinstance(obj, dict):
        for key in list(obj.keys()):
            normalized = normalize_dynamodb_write(key)
            obj[normalized] = (
                normalize_dynamodb_write(obj[key])
                if isinstance(obj[key], dict)
                else normalize_values(obj[key])
            )
            if key is not normalized:
                del obj[key]
    elif isinstance(obj, int) or isinstance(obj, Decimal) or isinstance(obj, float):
        return str(obj)
    return obj


def split_list(it: Iterable, size: int) -> list:
    """Splits given list in chunks of given size."""
    it = iter(it)
    return list(iter(lambda: tuple(islice(it, size)), ()))
