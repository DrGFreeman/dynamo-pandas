import pandas as pd
import pytest
from test_data import test_df

from dynamo_pandas import to_df
from dynamo_pandas import to_item
from dynamo_pandas import to_items

# List of item dictionaries with pandas dtypes
test_items_pd = test_df.to_dict("records")

# List of item dictionaries with types as returned from DyanamoDB when using the
# functions in the transactions module.
test_items = test_df.astype(dict(C="str", D="str", E="str", F="float")).to_dict(
    "records"
)


class Test_to_df:
    """Test the to_df function."""

    def test_single_item_dict(self):
        """Test conversion of a single item dictionary."""
        df = to_df(test_items[0])

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
        df = to_df(test_items)

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

    def test_with_dtype(self):
        """Test with dtype parameter specified."""
        df = to_df(
            test_items, dtype=dict(D="datetime64", E="datetime64[ns, UTC]", F="Int32")
        )

        assert [t.name for t in df.dtypes] == [
            "object",
            "int64",
            "object",
            "datetime64[ns]",
            "datetime64[ns, UTC]",
            "Int32",
            "float64",
            "int64",
        ]


class Test_to_item:
    """Test the to_item function."""

    @pytest.mark.parametrize("id", range(3))
    def test_df_single_row(self, id):
        """Test conversion of a single dataframe row."""
        item = to_item(test_df.loc[test_df.id == id])

        if id == 0:
            assert item == test_items_pd[id]
        else:
            # Compare dictionary string values as pd.NA and np.nan fail comparison
            assert str(item) == str(test_items_pd[id])

    def test_df_multiple_rows_raises(self):
        """Test that a dataframe with multiple rows raises a ValueError."""
        with pytest.raises(
            ValueError,
            match="obj must be a single row dataframe. Use the to_items function",
        ):
            to_item(test_df)

    def test_series(self):
        """Test conversion of a pandas series."""
        item = to_item(test_df.iloc[0])

        assert item == test_items_pd[0]

    def test_invalid_type_raises(self):
        """Test that a type other than a dataframe or series raises a TypeError."""
        with pytest.raises(
            TypeError,
            match="obj must be a pandas Series or a single row pandas DataFrame",
        ):
            to_item(test_items_pd[0])


class Test_to_items:
    """Test the to_items function."""

    def test_df(self):
        """Test that a dataframe converts to a list of item dictionaries."""
        items = to_items(test_df)

        # Compare dictionary string values as pd.NA and np.nan fail comparison
        assert str(items) == str(test_items_pd)

    def test_invalid_type_raises(self):
        """Test that a type different than a DataFrame raises a TypeError."""
        with pytest.raises(TypeError, match="df must be a pandas DataFrame"):
            to_items(test_items_pd)
