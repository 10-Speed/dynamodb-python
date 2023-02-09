import re
from decimal import Decimal
from itertools import islice
from typing import Any, Iterable

from boto3.dynamodb.conditions import And, Attr, Key, Not, Or
from requests import RequestException


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


def get_logical_operation(op: str):
    if op == "and":
        return And
    elif op == "or":
        return Or
    elif op == "not":
        return Not
    else:
        raise RequestException(f"{op} is not a valid logical operator (and, or, not).")


def get_operation(op: str):
    """Operation function"""
    if op == "eq":
        return Attr.eq
    if op == "ne":
        return Attr.ne
    elif op == "lt":
        return Attr.lt
    elif op == "gt":
        return Attr.gt
    elif op == "lte":
        return Attr.lte
    elif op == "gte":
        return Attr.gte
    elif op == "between":
        return Attr.between
    elif op == "begins_with":
        return Attr.begins_with
    elif op == "contains":
        return Attr.contains
    elif op == "exists":
        return Attr.exists
    elif op == "not_exists":
        return Attr.not_exists
    elif op == "in":
        return Attr.is_in


def make_expression(filter_expression: dict, is_key: bool = False) -> Attr:
    """Given {key, op, value} map - make filter expression."""
    attr = Attr(filter_expression["key"]) if not is_key else Key(filter_expression["key"])
    op = get_operation(filter_expression["op"])
    value = filter_expression.get("value", None)
    if not value:
        return op(attr)
    elif isinstance(value, (list, tuple)):
        return op(attr, *value)
    else:
        return op(attr, value)


def sanitize_name(name: str) -> str:
    """Sanitize name to be used as a key in DynamoDB."""
    return re.sub(r"[-\.]", "_", name)
