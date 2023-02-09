import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

import boto3
import botocore
from boto3.dynamodb.conditions import And, ConditionExpressionBuilder, Key
from ddbcereal import Deserializer, Serializer
from ddbcereal.types import PythonNumber
from requests import RequestException
from utils import get_logical_operation, make_expression, normalize_dynamodb_write, split_list


@dataclass()
class Table:
    tablename: str
    _client: boto3.client

    def __post_init__(self):
        self._exp_builder = ConditionExpressionBuilder()
        self._serializer = Serializer()
        self._deserializer = Deserializer(number_type=PythonNumber.INT_OR_DECIMAL)

    def _serialize(self, value: Any, explicit_value: bool = False) -> Union[Dict, List]:
        """Serializes value to DynamoDB structure.

        Explicit value indicates that this value (list or dict) is a value.
        Otherwise, list is interpreted as list of values and dict is interpreted as item
        rather than "value".
        """
        if isinstance(value, dict) and not explicit_value:
            return self._serializer.serialize_item(value)
        if isinstance(value, list) and not explicit_value:
            return [self._serializer.serialize_item(item) for item in value]
        else:
            return self._serializer.serialize(value)

    def _deserialize(self, value: Union[Dict, List], explicit_value: bool = False) -> Any:
        """Serializes value to DynamoDB structure.

        Explicit value indicates that this value (list or dict) is a value.
        Otherwise, list is interpreted as list of values and dict is interpreted as item
        rather than "value".
        """
        if isinstance(value, dict) and not explicit_value:
            return self._deserializer.deserialize_item(value)
        if isinstance(value, list) and not explicit_value:
            return [self._deserializer.deserialize_item(item) for item in value]
        else:
            return self._deserializer.deserialize(value)

    def read_item(self, key: Dict) -> Dict:
        """Read item with given keys. Must contain partition key and sort key if applicable."""
        response = self._client.get_item(TableName=self.tablename, Key=self._serialize(key))

        if "Item" not in response:
            raise KeyError("Item was not found.")

        return self._deserialize(response["Item"])

    def read_items(
        self,
        key: Tuple[str, str],
        filters: Optional[List] = None,
        ascending: Optional[bool] = False,
    ) -> list:
        """Read items with given key and filters. Key can be a key expression"""
        return self._read_items_more(key, filters, ascending)[0]

    def _read_items_more(
        self,
        key: Union[Key, Tuple[str, str]],
        filters: Optional[List] = None,
        ascending: bool = False,
        limit: int = None,
        last: Dict = None,
    ) -> tuple:
        """Read items with given key and filters. Key and filters can be a key expression"""
        if isinstance(key, Tuple):
            key_exp = Key(key[0]).eq(key[1])

        kwargs = dict(ExpressionAttributeNames={}, ExpressionAttributeValues={})

        # TODO: rewrite this logic
        if filters:
            filter_exp = None
            conditional_filter_exp = None

            for filter_ in filters:
                is_conditional_filter = not filter_.get("key")
                if is_conditional_filter:
                    operators = list(filter_.keys())
                    if len(operators) > 1:
                        raise RequestException(
                            "Objects containing logical operators are limited to one key."
                        )

                    operator = operators[0]
                    op_fn = get_logical_operation(operator)
                    op_filters = list(filter_.values())[0]
                    conditional_exps = [make_expression(f) for f in op_filters]
                    for exp in conditional_exps:
                        if operator == "not":
                            conditional_filter_exp = (
                                conditional_filter_exp & op_fn(exp)
                                if conditional_filter_exp
                                else op_fn(exp)
                            )
                        else:
                            conditional_filter_exp = (
                                op_fn(conditional_filter_exp, exp)
                                if conditional_filter_exp
                                else exp
                            )
                else:
                    filter_exp = (
                        filter_exp & make_expression(filter_)
                        if filter_exp
                        else make_expression(filter_)
                    )

            if conditional_filter_exp and filter_exp:
                filter_exp = And(filter_exp, conditional_filter_exp)
            elif conditional_filter_exp:
                filter_exp = conditional_filter_exp

            if filter_exp:
                filter_exp = self._exp_builder.build_expression(filter_exp, is_key_condition=False)
                kwargs["FilterExpression"] = filter_exp.condition_expression
                kwargs["ExpressionAttributeNames"].update(filter_exp.attribute_name_placeholders)
                kwargs["ExpressionAttributeValues"].update(filter_exp.attribute_value_placeholders)

        key_exp = self._exp_builder.build_expression(key_exp, is_key_condition=True)
        kwargs.update(
            dict(
                KeyConditionExpression=key_exp.condition_expression,
                ScanIndexForward=ascending,
            )
        )
        kwargs["ExpressionAttributeNames"].update(key_exp.attribute_name_placeholders)
        kwargs["ExpressionAttributeValues"].update(key_exp.attribute_value_placeholders)

        kwargs["ExpressionAttributeValues"] = self._serialize((kwargs["ExpressionAttributeValues"]))

        if limit or limit == 0:
            limit = min(max(0, limit), 200)
            if limit == 0:
                return [], {}
            kwargs["Limit"] = limit * 2

        if last:
            kwargs["ExclusiveStartKey"] = self._serialize(
                (
                    {
                        "key": last["key"],
                        "time": int(last["time"]),
                    }
                )
            )

        items = []
        last_key = {}
        parse_more = True

        while parse_more:
            response = self._client.query(TableName=self.tablename, **kwargs)
            items.extend(self._deserialize(response["Items"]))
            last_key = response.get("LastEvaluatedKey", {})
            parse_more = ((limit and len(items) < limit) or limit is None) and last_key
            kwargs["ExclusiveStartKey"] = last_key

        if limit and len(items) > limit:
            truncated_items = items[:limit]
            last_key = truncated_items[-1]
            return truncated_items, last_key
        return items, self._deserialize(last_key)

    def write(
        self,
        key: Dict,
        data: Dict,
        return_old_values: bool = False,
        ttl: Tuple[str, datetime] = None,
    ) -> dict:
        """Writing to DynamoDB database
        Can pass a ttl tuple (attribute_name, datetime) to set a ttl on the item.
        Optionally returns old values if return_old_values is True"""
        item = {**key, **data}

        if ttl:
            item[ttl[0]] = int(ttl[1].timestamp())

        return_values = "ALL_OLD" if return_old_values else "NONE"

        normalized_item = self._serialize((normalize_dynamodb_write(item)))
        old_values = self._client.put_item(
            TableName=self.tablename,
            Item=normalized_item,
            ReturnValues=return_values,
        )

        if return_old_values:
            normalized_item = old_values["Attributes"]

        return self._deserialize(normalized_item)

    def write_batch(self, items: List, write_op: bool = True) -> None:
        """Write list of items."""

        if not items:
            return

        if write_op:
            items = [{"PutRequest": {"Item": self._serialize(item)}} for item in items]
        else:
            items = [{"DeleteRequest": {"Key": self._serialize(item)}} for item in items]

        # If given list exceeds 25 items, need to split it into multiple batch reads,
        # since AWS only supports maximum of 25 items or 16MB batch reads.
        if len(items) > 25:
            split_items = split_list(items, 25)
        else:
            split_items = [items]

        for batch in split_items:
            response = self._client.batch_write_item(RequestItems={self.tablename: batch})

            # If batch read exceeded limits and returned UnprocessedKeys, add them to read queue.
            unprocessed_keys = response.get("UnprocessedKeys", {})
            if unprocessed_keys:
                split_items.append(unprocessed_keys[self.tablename]["Keys"])

    def write_batch_migrate(self, items: List):
        """Write list of items with write capacity exceptions in mind."""
        try:
            self.write_batch(items=items)
        except Exception as err:
            if err.response["ResponseMetadata"]["MaxAttemptsReached"]:
                time.sleep(120)
                self.write_batch(items=items)
            else:
                print(err)

    def write_list(self, keys: List, data: Dict) -> Dict:
        """Write same data with to multiple keys."""
        response = {}
        for key in keys:
            response = self.write(key, data)
        return response

    def exists(self, key: Dict) -> bool:
        """Check whether item with such key exists."""
        try:
            self.read_item(key)
            return True
        except KeyError:
            return False

    def delete(self, key: Dict) -> Dict:
        """Delete item and return its values."""
        response = self._client.delete_item(
            TableName=self.tablename,
            Key=self._serialize(key),
            ReturnValues="ALL_OLD",
        )
        if "Attributes" not in response:
            raise KeyError("Key was not found.")
        return self._deserialize(response["Attributes"])

    def delete_list(self, keys: List[Dict]) -> Dict:
        """Delete multiple keys."""
        response = {}
        for key in keys:
            response = self.delete(key)
        return response

    def delete_batch(self, keys: List[Dict]) -> None:
        """Delete given list of items."""
        self.write_batch(keys, write_op=False)

    def read_batch(self, items: List, request_params: Optional[Dict] = None) -> List:
        """Read records in batch based on given list of items. An item in this context is
        a dict with the key and time of required record in Dynamo."""
        all_responses = []

        items = [self._serialize((item)) for item in items]

        # If given list exceeds 100 items, need to split it into multiple batch reads,
        # since AWS only supports maximum of 100 items or 16MB batch reads.
        if len(items) > 100:
            split_items = split_list(items, 100)
        else:
            split_items = [items]

        if not request_params:
            request_params = {}

        for batch in split_items:
            response = self._client.batch_get_item(
                RequestItems={self.tablename: {**request_params, "Keys": batch}}
            )

            responses = response["Responses"][self.tablename]
            all_responses.append(self._deserialize(responses))

            # If batch read exceeded limits and returned UnprocessedKeys, add them to read queue.
            unprocessed_keys = response.get("UnprocessedKeys", {})
            if unprocessed_keys:
                split_items.append(unprocessed_keys[self.tablename]["Keys"])

        # Flatten all batch responses into a single list and return.
        return [item for batch in all_responses for item in batch]

    def update(self, key: Dict, field: str, value: Any, attributes: Dict = None) -> Dict:
        """
        Updates given field with given value.
        Initializes field if it did not exist yet.
        Returns whole record attributes after the update.

        Only supports int field updates!

        Attributes can be used to overcome DynamoDB limitation of string only fields.
        You can provide "foo.#val" with attributes={#val: str(100)} instead of "foo.100" which
        results in an error.

        Example:
            _update(
                key="stats_count",
                time=0,
                field="recruitment_states.#state",
                attributes={#state: str(100), value=1}
            )
        """
        kwargs = {}
        if attributes:
            kwargs["ExpressionAttributeNames"] = attributes

        key_exp = {k: self._serialize(v) for k, v in key.items()}
        response = self._client.update_item(
            TableName=self.tablename,
            Key=key_exp,
            UpdateExpression=f"add {field} :value",
            # TODO This will break if field is reserved keyword.
            #  botocore classes should be used for constructing those time of things.
            ExpressionAttributeValues={
                ":value": self._serialize(value, explicit_value=True),
            },
            ReturnValues="ALL_NEW",
            **kwargs,
        )

        return response["Attributes"]

    def increment(self, key: Dict, field: str, attributes: Dict = None) -> Dict:
        """
        Increment a value by one given access keys and a field. Nested fields are separated by dots:
            dict["a"]["b"] should be passed as field="a.b"
        """
        return self.update(key, field, 1, attributes)

    def decrement(self, key: Dict, field: str, attributes: dict = None) -> Dict:
        """
        Decrement a value by one given access keys and a field. Nested fields are separated by dots:
            dict["a"]["b"] should be passed as field="a.b"
        """
        return self.update(key, field, -1, attributes)

    def init_empty_map(self, key: Dict, field: str, attributes: Dict = None) -> Dict:
        """Initialize a field with an empty map if it does not exist yet, otherwise do nothing.

        Attributes can be used to overcome DynamoDB limitation of string only fields.
        You can provide "foo.#val" with attributes={#val: str(100)} instead of "foo.100" which
        results in an error.

        Example:
            _init_empty_map(
                key="stats_count", time=0, field="by_source.#source", attributes={#source: str(1)}
            )
        """
        kwargs = {}
        if attributes:
            kwargs["ExpressionAttributeNames"] = attributes

        key_exp = {k: self._serialize(v) for k, v in key.items()}
        try:
            response = self._client.update_item(
                TableName=self.tablename,
                Key=key_exp,
                UpdateExpression=f"set {field} = if_not_exists({field}, :value)",
                # TODO This will break if field is reserved keyword.
                #  botocore classes should be used for constructing those sort of things.
                ExpressionAttributeValues={
                    ":value": self._serialize({}, explicit_value=True),
                },
                ReturnValues="ALL_NEW",
                **kwargs,
            )
            return self._deserialize(response["Attributes"])
        except botocore.errorfactory.ClientError as e:
            # Failure due to existing attribute ignored on purpose.
            if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
