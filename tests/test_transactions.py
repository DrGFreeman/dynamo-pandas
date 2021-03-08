import base64
import math
import os
import random
import re
from string import ascii_letters
import sys

import pandas as pd
import pytest

from dynamo_pandas.transactions import get_all_items
from dynamo_pandas.transactions import get_item
from dynamo_pandas.transactions import get_items
from dynamo_pandas.transactions import keys
from dynamo_pandas.transactions import put_item

"""Define a test dataframe with mixed types and missing values:
>>> print(test_df)
      A  B               C          D                         E     F  \
0   abc  2 0 days 19:32:01 2000-01-01 2000-01-01 00:00:00+00:00   128   
1  None  3 1 days 01:33:20 2000-12-31 2000-12-31 23:59:59+00:00  <NA>   
2   NaN  4 2 days 23:06:40        NaT                       NaT  <NA>   

          G  id  
0  3.141593   0  
1       NaN   1  
2       NaN   2  
>>> test_df.info()
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 3 entries, 0 to 2
Data columns (total 8 columns):
 #   Column  Non-Null Count  Dtype              
---  ------  --------------  -----              
 0   A       1 non-null      object             
 1   B       3 non-null      int64              
 2   C       3 non-null      timedelta64[ns]    
 3   D       2 non-null      datetime64[ns]     
 4   E       2 non-null      datetime64[ns, UTC]
 5   F       1 non-null      Int32              
 6   G       1 non-null      float64            
 7   id      3 non-null      int64              
dtypes: Int32(1), datetime64[ns, UTC](1), datetime64[ns](1), float64(1), int64(2), object(1), timedelta64[ns](1)
memory usage: 311.0+ bytes"""  # noqa: E501, W291
test_df = pd.DataFrame(
    [
        {
            "A": "abc",
            "B": 2,
            "C": 70321.4,
            "D": "2000-01-01",
            "E": "2000-01-01",
            "F": 128,
            "G": math.pi,
        },
        {
            "A": None,
            "B": 3,
            "C": 92000,
            "D": "2000-12-31",
            "E": "2000-12-31 23:59:59",
            "F": None,
            "G": None,
        },
        {"B": 4, "C": 256000},
    ]
).astype(
    {
        "C": "timedelta64[s]",
        "D": "datetime64[ns]",
        "E": "datetime64[ns, UTC]",
        "F": "Int32",
    }
)
test_df["id"] = range(len(test_df))

large_table_items = [
    dict(id=i, letter=random.choice(ascii_letters), number=random.randint(0, 1000))
    for i in range(250)
]


@pytest.fixture()
def empty_table(ddb_client):
    """Fixture generating an empty table and yielding the name of the table. The table
    primary key is named 'id' and is of numerical type."""
    table_name = "test-empty-table"
    response = ddb_client.create_table(
        AttributeDefinitions=[dict(AttributeName="id", AttributeType="N")],
        TableName=table_name,
        KeySchema=[dict(AttributeName="id", KeyType="HASH")],
        BillingMode="PROVISIONED",
        ProvisionedThroughput=dict(ReadCapacityUnits=5, WriteCapacityUnits=5),
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    yield table_name


@pytest.fixture()
def test_df_table(ddb_client):
    """Fixture generating a table with the items from test_df and yielding the name of
    the table. The table primary key is named 'id' and is of numerical type."""
    table_name = "test-df-table"
    response = ddb_client.create_table(
        AttributeDefinitions=[dict(AttributeName="id", AttributeType="N")],
        TableName=table_name,
        KeySchema=[dict(AttributeName="id", KeyType="HASH")],
        BillingMode="PROVISIONED",
        ProvisionedThroughput=dict(ReadCapacityUnits=5, WriteCapacityUnits=5),
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    for item in test_df.to_dict("records"):
        response = put_item(item=item, table=table_name, return_response=True)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    yield table_name


@pytest.fixture()
def large_table(ddb_client):
    """Fixture providing a large table (250 items) to test get operations with more
    than 100 items. The table primary key is named 'id' and is of numerical type."""
    table_name = "test-large-table"
    response = ddb_client.create_table(
        AttributeDefinitions=[dict(AttributeName="id", AttributeType="N")],
        TableName=table_name,
        KeySchema=[dict(AttributeName="id", KeyType="HASH")],
        BillingMode="PROVISIONED",
        ProvisionedThroughput=dict(ReadCapacityUnits=5, WriteCapacityUnits=5),
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    for item in large_table_items:
        response = put_item(item=item, table=table_name, return_response=True)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    yield table_name


@pytest.fixture()
def large_objects_table(ddb_client):
    """Fixture providing a table with large objects (~395 KB each) to test get
    operations with more generating large data sizes. The table primary key is named
    'id' and is of numerical type."""
    table_name = "test-large-objects-table"
    response = ddb_client.create_table(
        AttributeDefinitions=[dict(AttributeName="id", AttributeType="N")],
        TableName=table_name,
        KeySchema=[dict(AttributeName="id", KeyType="HASH")],
        BillingMode="PAY_PER_REQUEST",
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    for i in range(100):
        item = dict(id=i, A=base64.b64encode(os.urandom(296 * 1024)).decode())
        response = put_item(item=item, table=table_name, return_response=True)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    yield table_name


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
