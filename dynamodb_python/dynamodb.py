from dataclasses import dataclass
from typing import ClassVar, Dict, List, Optional

import boto3
import botocore
from botocore.config import Config
from table import Table
from utils import sanitize_name


AUTH_NAME = "SYSTEM"  # TODO: change this
AWS_TABLENAME_KEY = "TableNames"


@dataclass
class DynamoDB:
    """Base for communications with DynamoDB.

    Raises:
        KeyError: 'Item' not found in response for _read_item or 'Items' in _read_items.
    """

    DATE_FORMAT: ClassVar[str] = "%Y%m%d"
    YEAR_WEEK_FORMAT: ClassVar[str] = "%Y%W"
    YEAR_MONTH_FORMAT: ClassVar[str] = "%Y%m"

    tablenames: Optional[List] = None
    credentials: Optional[Dict] = None
    config: Optional[botocore.client.Config] = None  # TODO: should be able to pass a file

    def __post_init__(self):
        self._client = boto3.client(
            "dynamodb",
            config=self.config if self.config else None,
            **self.credentials if self.credentials else {},
        )

        self.tablenames = (
            self.tablenames if self.tablenames else self._client.list_tables()[AWS_TABLENAME_KEY]
        )

        for name in self.tablenames:
            setattr(self, sanitize_name(name), Table(name, self._client))


if __name__ == "__main__":
    # test example
    dynamodb = DynamoDB(config=Config(retries={"max_attempts": 5, "mode": "standard"}))
    dynamodb.table_name.write(key="test", time=50, data={})
