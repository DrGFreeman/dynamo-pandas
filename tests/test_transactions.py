import base64
import os
import re
import sys

import pandas as pd
import pytest
from test_data import large_table_items
from test_data import test_df

from dynamo_pandas.transactions import get_all_items
from dynamo_pandas.transactions import get_item
from dynamo_pandas.transactions import get_items
from dynamo_pandas.transactions import keys
from dynamo_pandas.transactions import put_item
from dynamo_pandas.transactions import put_items


class Test_keys:
    """Test the keys function."""

    def test_partition_key_only(self):
        """Test that the keys function works with only a partition key."""
        assert keys(id=range(3)) == [{"id": 0}, {"id": 1}, {"id": 2}]

    def test_two_or_more_kwargs_raises(self):
        """Test that two or more keyword arguments raises a ValueError."""
        with pytest.raises(
            ValueError,
            match=re.escape("Only one key attribute (partition key) is supported."),
        ):
            keys(id=[1, 2, 3], di=[3, 2, 1])


class Test_put_item:
    """Test the put_item function."""

    def test_return_response(self, ddb_client, empty_table):
        """Test that setting return_response to True returns the put_item response."""
        resp = put_item(item=dict(id=0, A=1), table=empty_table, return_response=True)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_simple_item(self, ddb_client, empty_table):
        """Test that a simple item is properly added to the table"""
        put_item(item=dict(id=1, A=2), table=empty_table)

        resp = ddb_client.get_item(TableName=empty_table, Key=dict(id=dict(N="1")))
        assert resp["Item"] == {"id": {"N": "1"}, "A": {"N": "2"}}

    @pytest.mark.parametrize(
        ["column", "id", "expected"],
        [
            # object (str)
            ("A", 0, {"id": {"N": "0"}, "A": {"S": "abc"}}),
            # object (Nonetype)
            ("A", 1, {"id": {"N": "1"}, "A": {"NULL": True}}),
            # object, missing value (NaN)
            ("A", 2, {"id": {"N": "2"}, "A": {"NULL": True}}),
            # int, no missing values (numpy.int64)
            ("B", 0, {"id": {"N": "0"}, "B": {"N": "2"}}),
            # Timedelta, no missing values
            ("C", 0, {"id": {"N": "0"}, "C": {"S": "0 days 19:32:01"}}),
            # Timestamp, date only
            ("D", 0, {"id": {"N": "0"}, "D": {"S": "2000-01-01 00:00:00"}}),
            # Timestamp, missing value (NaT)
            ("D", 2, {"id": {"N": "2"}, "D": {"NULL": True}}),
            # Timestamp UTC, date only
            ("E", 0, {"id": {"N": "0"}, "E": {"S": "2000-01-01 00:00:00+00:00"}}),
            # Timestamp UTC, date and time
            ("E", 1, {"id": {"N": "1"}, "E": {"S": "2000-12-31 23:59:59+00:00"}}),
            # Timestamp UTC, missing value (NaT)
            ("E", 2, {"id": {"N": "2"}, "E": {"NULL": True}}),
            # pandas Nullable Integer (Int32)
            ("F", 0, {"id": {"N": "0"}, "F": {"N": "128"}}),
            # pandas Nullable Integer, missing value (pd.NA)
            ("F", 1, {"id": {"N": "1"}, "F": {"NULL": True}}),
            # float64 (numpy.float64)
            ("G", 0, {"id": {"N": "0"}, "G": {"N": "3.141592653589793"}}),
            # float64, missing value (np.nan)
            ("G", 1, {"id": {"N": "1"}, "G": {"NULL": True}}),
        ],
    )
    def test_from_df_types(self, ddb_client, empty_table, column, id, expected):
        """Test that an item with pandas dtypes is properly added to the table."""
        item = test_df[["id", column]].iloc[id].to_dict()
        put_item(item=item, table=empty_table)

        resp = ddb_client.get_item(TableName=empty_table, Key=dict(id=dict(N=str(id))))
        assert resp["Item"] == expected


@pytest.mark.parametrize(
    ["id", "expected"],
    [
        (
            0,
            {
                "A": "abc",
                "B": 2,
                "C": "0 days 19:32:01",
                "D": "2000-01-01 00:00:00",
                "E": "2000-01-01 00:00:00+00:00",
                "F": 128,
                "G": 3.141592653589793,
                "id": 0,
            },
        ),
        (
            1,
            {
                "A": None,
                "B": 3,
                "C": "1 days 01:33:20",
                "D": "2000-12-31 00:00:00",
                "E": "2000-12-31 23:59:59+00:00",
                "F": None,
                "G": None,
                "id": 1,
            },
        ),
        (
            2,
            {
                "A": None,
                "B": 4,
                "C": "2 days 23:06:40",
                "D": None,
                "E": None,
                "F": None,
                "G": None,
                "id": 2,
            },
        ),
        (3, None),
    ],
)
def test_get_item(ddb_client, test_df_table, id, expected):
    """Test the get_item function. Compare the returned item dictionary with the
    expected one for each item in test_df."""
    item = get_item(key=dict(id=id), table=test_df_table)

    assert item == expected


class Test_get_items:
    """Test the get_items function."""

    def test_multiple_existing(self, ddb_client, test_df_table):
        """Test with multiple existing items."""
        items = get_items(keys=[{"id": 0}, {"id": 2}], table=test_df_table)
        assert items == [
            {
                "A": "abc",
                "B": 2,
                "C": "0 days 19:32:01",
                "D": "2000-01-01 00:00:00",
                "E": "2000-01-01 00:00:00+00:00",
                "F": 128,
                "G": 3.141592653589793,
                "id": 0,
            },
            {
                "A": None,
                "B": 4,
                "C": "2 days 23:06:40",
                "D": None,
                "E": None,
                "F": None,
                "G": None,
                "id": 2,
            },
        ]

    def test_multiple_missing(self, ddb_client, test_df_table):
        """Test with multiple items, some non-existent."""
        items = get_items(keys=[{"id": 0}, {"id": 3}], table=test_df_table)
        assert items == [
            {
                "A": "abc",
                "B": 2,
                "C": "0 days 19:32:01",
                "D": "2000-01-01 00:00:00",
                "E": "2000-01-01 00:00:00+00:00",
                "F": 128,
                "G": 3.141592653589793,
                "id": 0,
            }
        ]

    def test_single_existing(self, ddb_client, test_df_table):
        """Test with a single existing item."""
        items = get_items(keys=[{"id": 0}], table=test_df_table)
        assert items == [
            {
                "A": "abc",
                "B": 2,
                "C": "0 days 19:32:01",
                "D": "2000-01-01 00:00:00",
                "E": "2000-01-01 00:00:00+00:00",
                "F": 128,
                "G": 3.141592653589793,
                "id": 0,
            }
        ]

    def test_single_missing(self, ddb_client, test_df_table):
        """Test with a single non-existent item."""
        items = get_items(keys=[{"id": 3}], table=test_df_table)
        assert items == []

    def test_large_table(self, ddb_client, large_table):
        """Test that it is possible to get more than 100 items in a single call
        (DynamoDB limit for batch_get_item method). Also verify that the items are
        returned in the order their keys are specified."""
        ids = list(pd.DataFrame(large_table_items).id.sample(125))

        items = get_items(keys=keys(id=ids), table=large_table)

        assert len(items) == len(ids)
        assert [item["id"] for item in items] == ids

    def test_large_objects(self, ddb_client, large_objects_table):
        """Test that it is possible to get more than 16 MB of data in a single call
        (DynamoDB limit for batch_get_item method)."""
        ids = list(range(99))

        items = get_items(keys=keys(id=ids), table=large_objects_table)

        assert len(items) == len(ids)
        assert [item["id"] for item in items] == ids

        size = sum(sys.getsizeof(i["id"]) + sys.getsizeof(i["A"]) for i in items)
        assert size / 1024 / 1024 > 16


class Test_get_all_items:
    """Test the get_all_items function."""

    def test_mixed_types(self, ddb_client, test_df_table):
        """Test that the items from test_df_table are returned with the correct data
        types."""
        items = get_all_items(table=test_df_table)

        assert items == [
            {
                "A": "abc",
                "B": 2,
                "C": "0 days 19:32:01",
                "D": "2000-01-01 00:00:00",
                "E": "2000-01-01 00:00:00+00:00",
                "F": 128,
                "G": 3.141592653589793,
                "id": 0,
            },
            {
                "A": None,
                "B": 3,
                "C": "1 days 01:33:20",
                "D": "2000-12-31 00:00:00",
                "E": "2000-12-31 23:59:59+00:00",
                "F": None,
                "G": None,
                "id": 1,
            },
            {
                "A": None,
                "B": 4,
                "C": "2 days 23:06:40",
                "D": None,
                "E": None,
                "F": None,
                "G": None,
                "id": 2,
            },
        ]

    def test_large_objects_table(self, ddb_client, large_objects_table):
        """Test that more than 1 MB of items can be returned (DynamoDB limit for table
        scan operations)."""
        items = get_all_items(table=large_objects_table)

        assert len(items) == 100
        size = sum(sys.getsizeof(i["id"]) + sys.getsizeof(i["A"]) for i in items)
        assert size / 1024 / 1024 > 1


class Test_put_items:
    """Test the put_items function."""

    def test_simple_types(self, ddb_client, empty_table):
        """Test with a simple list of items with stardard types."""
        items = [dict(A="A", B=1, id=0), dict(A="B", B=2, id=1)]

        put_items(items=items, table=empty_table)

        assert get_all_items(table=empty_table) == items

    def test_from_df_types(self, ddb_client, empty_table):
        """Test with pandas data types from test_df."""
        items = test_df.to_dict("records")

        put_items(items=items, table=empty_table)

    def test_large_number_of_items(self, ddb_client, empty_table):
        """Test with more than 25 items (DynamoDB batch size limit for
        batch_write_item)."""
        items = large_table_items

        put_items(items=items, table=empty_table)

        assert len(get_all_items(table=empty_table)) == len(items)

    def test_large_items(self, ddb_client, empty_table):
        """Test with large items so that total request size exceeds 16 MB (DynamoDB
        batch size limit for batch_write_item)."""
        items = [
            dict(id=i, A=base64.b64encode(os.urandom(296 * 1024)).decode())
            for i in range(100)
        ]

        put_items(items=items, table=empty_table)

        assert len(get_all_items(table=empty_table)) == len(items)

    def test_item_not_a_list_raises(self, ddb_client, empty_table):
        """Test that a TypeError is raised if items is not a list."""
        with pytest.raises(
            TypeError, match="items must be a list of non-empty dictionaries"
        ):
            put_items(items=large_table_items[0], table=empty_table)
