import pytest
from test_data import test_df

from dynamo_pandas import to_item
from dynamo_pandas import to_items


class Test_to_item:
    """Test the to_item function."""

    @pytest.mark.parametrize("id", range(3))
    def test_df_single_row(self, id):
        """Test conversion of a single dataframe row."""
        item = to_item(test_df.loc[test_df.id == id])

        if id == 0:
            assert item == test_df.to_dict("records")[id]
        else:
            # Compare dictionary string values as pd.NA and np.nan fail comparison
            assert str(item) == str(test_df.to_dict("records")[id])

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

        assert item == test_df.to_dict("records")[0]

    def test_invalid_type_raises(self):
        """Test that a type other than a dataframe or series raises a TypeError."""
        with pytest.raises(
            TypeError,
            match="obj must be a pandas Series or a single row pandas DataFrame",
        ):
            to_item(test_df.to_dict("records")[0])


class Test_to_items:
    """Test the to_items function."""

    def test_df(self):
        """Test that a dataframe converts to a list of item dictionaries."""
        items = to_items(test_df)

        # Compare dictionary string values as pd.NA and np.nan fail comparison
        assert str(items) == str(test_df.to_dict("records"))

    def test_invalid_type_raises(self):
        """Test that a type different than a DataFrame raises a TypeError."""
        with pytest.raises(TypeError, match="df must be a pandas DataFrame"):
            to_items(test_df.to_dict("records"))
