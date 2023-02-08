# dynamodb-python

## Installation

## Requirements

### Dependencies

- python = "^3.9"
- ddbcereal
- botocore
- boto3
- requests

### Credentials

You will first need to set your AWS credentials. Since this library uses `boto3` under the hood, you can use the same methods as described in the [boto3 documentation](https://boto3.readthedocs.io/en/latest/guide/configuration.html). In short, AWS looks for credentials in these places:

1. Environment variables
2. Shared credentials file (`~/.aws/credentials`)
3. AWS config file (`~/.aws/config`)

You cacn also pass a dictionary to `DynamoDB` class:

```python
from dynamodb_python import DynamoDB

dynamodb = DynamoDB(credentials={
    "aws_access_key_id": ACCESS_KEY,
    "aws_secret_access_key": SECRET_KEY,
    "aws_session_token": SESSION_TOKEN
})
```

## How to use

Having a table called `table_name`, you can access it like this:

```python
from dynamodb_python import DynamoDB

dynamodb = DynamoDB()
table = dynamodb.table_name
```

1. And then you can get an item like this:

```python
table.read_item(key="key", time=timestamp)
```

2. Or you can get a list of items like this:

```python
table.read_items(key="key")
```

3. Or you can write an item like this:

```python
table.write(key="key", time=timestamp, data={"data": "data"})
```
