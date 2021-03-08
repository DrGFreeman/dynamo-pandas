import pandas as pd


def to_df(items, *, dtype=None):
    """Convert an item dictionary or list of item dictionaries into a pandas DataFrame.
    """
    if isinstance(items, dict):
        items = [items]

    df = pd.DataFrame(items)

    if dtype is not None:
        df = df.astype(dtype)

    return df


def to_items(df):
    """Convert a pandas dataframe to a dictionary of items."""
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame")

    return df.to_dict("records")


def to_item(obj):
    """Convert a pandas Series or a single row pandas DataFrame to an item dictionary.
    """
    if isinstance(obj, pd.DataFrame):
        if len(obj) != 1:
            raise ValueError(
                "obj must be a single row dataframe. Use the to_items function to "
                + "convert a multi row dataframe."
            )

        return obj.to_dict("records")[0]

    elif isinstance(obj, pd.Series):
        return obj.to_dict()

    else:
        raise TypeError("obj must be a pandas Series or a single row pandas DataFrame")
