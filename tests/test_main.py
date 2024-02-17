import re
from unittest import mock

from packaging.version import parse as parse_version
import pandas as pd
import pytest
from test_data import test_df

from dynamo_pandas import get_df
from dynamo_pandas import keys
from dynamo_pandas import put_df
from dynamo_pandas.dynamo_pandas import _to_df
from dynamo_pandas.dynamo_pandas import _to_items

# List of item dictionaries with pandas dtypes
test_items_pd = test_df.to_dict("records")

# List of item dictionaries with types as returned from DyanamoDB when using the
# functions in the transactions module.
test_items = test_df.astype(dict(C="str", D="str", E="str", F="float")).to_dict(
    "records"
)


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


class Test_get_df:
    """Test the get_df function."""

    def test_no_keys(self, test_df_table):
        """Test that not specifying keys returns all items."""
        df = get_df(table=test_df_table)

        assert {c: t.name for c, t in zip(df.columns, df.dtypes)} == {
            "A": "object",
            "B": "int64",
            "C": "object",
            "D": "object",
            "E": "object",
            "F": "float64",
            "G": "float64",
            "id": "int64",
        }
        assert df.equals(
            pd.DataFrame(
                [
                    {
                        "A": "abc",
                        "B": 2,
                        "C": "0 days 19:32:01",
                        "D": "2000-01-01 00:00:00",
                        "E": "2000-01-01 00:00:00+00:00",
                        "F": 128.0,
                        "G": 3.141592653589793,
                        "id": 0,
                    },
                    {
                        "A": None,
                        "B": 3,
                        "C": "1 days 01:33:20",
                        "D": "2000-12-31 00:00:00",
                        "E": "2000-12-31 23:59:59+00:00",
                        "id": 1,
                    },
                    {
                        "A": None,
                        "B": 4,
                        "C": "2 days 23:06:40",
                        "D": None,
                        "E": None,
                        "id": 2,
                    },
                ]
            )
        )

    def test_keys(self, test_df_table):
        """Test with keys specified. Also test that keys not in the table are
        ignored."""
        df = get_df(table=test_df_table, keys=[{"id": 0}, {"id": 2}, {"id": 3}])

        assert {c: t.name for c, t in zip(df.columns, df.dtypes)} == {
            "A": "object",
            "B": "int64",
            "C": "object",
            "D": "object",
            "E": "object",
            "F": "float64",
            "G": "float64",
            "id": "int64",
        }
        assert df.equals(
            pd.DataFrame(
                [
                    {
                        "A": "abc",
                        "B": 2,
                        "C": "0 days 19:32:01",
                        "D": "2000-01-01 00:00:00",
                        "E": "2000-01-01 00:00:00+00:00",
                        "F": 128.0,
                        "G": 3.141592653589793,
                        "id": 0,
                    },
                    {
                        "A": None,
                        "B": 4,
                        "C": "2 days 23:06:40",
                        "D": None,
                        "E": None,
                        "id": 2,
                    },
                ]
            )
        )

    def test_single_key_missing(self, test_df_table):
        """Test that a single key not in the table returns an empty dataframe."""
        df = get_df(table=test_df_table, keys=[dict(id=3)])

        assert df.empty

    def test_attributes_returns_specified_columns(self, test_df_table):
        """Test that only columns corresponding to the specified attributes are
        returned."""
        df = get_df(
            table=test_df_table,
            attributes=["id", "A", "E"],
            keys=[{"id": 0}, {"id": 1}],
        )

        assert df.equals(
            pd.DataFrame(
                [
                    {"id": 0, "A": "abc", "E": "2000-01-01 00:00:00+00:00"},
                    {"id": 1, "A": None, "E": "2000-12-31 23:59:59+00:00"},
                ]
            )
        )

    def test_attributes_and_keys(self, test_df_table):
        """Test that only columns corresponding to the specified attributes are
        returned along with keys."""
        df = get_df(table=test_df_table, attributes=["id", "A", "E"])

        assert df.equals(
            pd.DataFrame(
                [
                    {"id": 0, "A": "abc", "E": "2000-01-01 00:00:00+00:00"},
                    {"id": 1, "A": None, "E": "2000-12-31 23:59:59+00:00"},
                    {"id": 2, "A": None, "E": None},
                ]
            )
        )

    @pytest.mark.skipif(
        parse_version(pd.__version__) < parse_version("1.5"),
        reason="https://github.com/DrGFreeman/dynamo-pandas/issues/24",
    )
    def test_dtype(self, test_df_table):
        """Test that the dtype parameter controls the returned data types."""
        df = get_df(
            table=test_df_table,
            dtype={
                "C": "timedelta64[ns]",
                "D": "datetime64[ns]",
                "E": "datetime64[ns, UTC]",
                "F": "Int32",
            },
        )
        assert {c: t.name for c, t in zip(df.columns, df.dtypes)} == {
            "A": "object",
            "B": "int64",
            "C": "timedelta64[ns]",
            "D": "datetime64[ns]",
            "E": "datetime64[ns, UTC]",
            "F": "Int32",
            "G": "float64",
            "id": "int64",
        }

    @pytest.mark.parametrize("keys", (None, [{"id": 0}]))
    def test_boto3_kwargs_are_passed(self, ddb_client, test_df_table, keys):
        """Test that the boto3_kwargs are passed to the boto3.resource() function
        call."""
        # test_df_table is defined in us-east-1. By setting the region_name to
        # ca-central-1, we expect a ResourceNotFoundException.
        with pytest.raises(ddb_client.exceptions.ResourceNotFoundException):
            get_df(
                keys=keys,
                table=test_df_table,
                boto3_kwargs=dict(region_name="ca-central-1"),
            )

        # With the correct region we expect to get the items.
        df = get_df(
            keys=keys,
            table=test_df_table,
            boto3_kwargs=dict(region_name="us-east-1"),
        )
        assert not df.empty


class Test_put_df:
    """Test the put_df function."""

    def test_with_pd_types(self, ddb_client, empty_table):
        """Test putting a dataframe using pandas specific data types."""
        put_df(df=test_df, table=empty_table)

        assert get_df(table=empty_table).equals(
            pd.DataFrame(
                [
                    {
                        "A": "abc",
                        "B": 2,
                        "C": "0 days 19:32:01",
                        "D": "2000-01-01 00:00:00",
                        "E": "2000-01-01 00:00:00+00:00",
                        "F": 128.0,
                        "G": 3.141592653589793,
                        "id": 0,
                    },
                    {
                        "A": None,
                        "B": 3,
                        "C": "1 days 01:33:20",
                        "D": "2000-12-31 00:00:00",
                        "E": "2000-12-31 23:59:59+00:00",
                        "id": 1,
                    },
                    {
                        "A": None,
                        "B": 4,
                        "C": "2 days 23:06:40",
                        "D": None,
                        "E": None,
                        "id": 2,
                    },
                ]
            )
        )

    def test_boto3_kwargs_are_passed(self, ddb_client, empty_table):
        """Test that the boto3_kwargs are passed to the boto3.client() function call."""
        # Moto does not raise the expected ResourceNotFoundError (see
        # https://github.com/spulec/moto/issues/4347)
        # We mock the boto3.client call instead and verify the boto3_kwargs are passed
        with mock.patch(
            "dynamo_pandas.transactions.transactions.boto3.client"
        ) as client:
            put_df(
                test_df,
                table=empty_table,
                boto3_kwargs=dict(region_name="ca-central-1"),
            )

            assert client.call_args[1] == dict(region_name="ca-central-1")


class Test__to_df:
    """Test the _to_df function."""

    def test_single_item_dict(self):
        """Test conversion of a single item dictionary."""
        df = _to_df(test_items[0])

        assert df.equals(pd.DataFrame([test_items[0]]))
        assert [t.name for t in df.dtypes] == [
            "object",
            "int64",
            "object",
            "object",
            "object",
            "float64",
            "float64",
            "int64",
        ]

    def test_multiple_item_dicts(self):
        """Test conversion of a list of item dictionaries."""
        df = _to_df(test_items)

        assert df.equals(pd.DataFrame(test_items))
        assert [t.name for t in df.dtypes] == [
            "object",
            "int64",
            "object",
            "object",
            "object",
            "float64",
            "float64",
            "int64",
        ]

    @pytest.mark.skipif(
        parse_version(pd.__version__) < parse_version("1.5"),
        reason="https://github.com/DrGFreeman/dynamo-pandas/issues/24",
    )
    def test_with_dtype(self):
        """Test with dtype parameter specified."""
        df = _to_df(
            test_items,
            dtype=dict(
                C="timedelta64[ns]",
                D="datetime64[ns]",
                E="datetime64[ns, UTC]",
                F="Int32",
            ),
        )

        assert [t.name for t in df.dtypes] == [
            "object",
            "int64",
            "timedelta64[ns]",
            "datetime64[ns]",
            "datetime64[ns, UTC]",
            "Int32",
            "float64",
            "int64",
        ]


class Test__to_items:
    """Test the _to_items function."""

    def test_df(self):
        """Test that a dataframe converts to a list of item dictionaries."""
        items = _to_items(test_df)

        # Compare dictionary string values as pd.NA and np.nan fail comparison
        assert str(items) == str(test_items_pd)

    def test_invalid_type_raises(self):
        """Test that a type different than a DataFrame raises a TypeError."""
        with pytest.raises(TypeError, match="df must be a pandas DataFrame"):
            _to_items(test_items_pd)
