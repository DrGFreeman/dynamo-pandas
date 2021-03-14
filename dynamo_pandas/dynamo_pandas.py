import pandas as pd

from .transactions import get_all_items
from .transactions import get_items
from .transactions import put_items


def get_df(*, table, keys=None, dtype=None):
    """Get items from a table into a dataframe.

    Parameters
    ----------
    table : str
        Name of the DynamoDB table.

    keys : list[dict]
        List of keys to get where each key is represented by a dictionary.

    dtype : dict
        Data type for data or columns. E.g. {‘a’: np.float64, ‘b’: np.int32, ‘c’:
        ‘Int64’} Use str or object.

    Returns
    -------
    pandas.DataFrame
        A dataframe where each item from the table matching the requested keys is
        represented by a row and its attributes by columns.

    Examples
    --------

    >>> df = get_df(
    ...     table="players",
    ...     keys=[{"player_id": "player_three"}, {"player_id": "player_one"}]
    ... )
    >>> print(df)
       bonus_points     player_id            last_play  rating        play_time
    0             4  player_three  2021-01-21 10:22:43     2.5  1 days 14:01:19
    1             3    player_one  2021-01-18 22:47:23     4.3  2 days 17:41:55

    By default, the data types of the returned dataframe are basic pandas/numpy types:

    >>> df.info()
    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 2 entries, 0 to 1
    Data columns (total 5 columns):
        #   Column        Non-Null Count  Dtype
       ---  ------        --------------  -----
        0   bonus_points  1 non-null      float64
        1   player_id     2 non-null      object
        2   last_play     2 non-null      object
        3   rating        2 non-null      float64
        4   play_time     2 non-null      object
    dtypes: float64(2), object(3)
    memory usage: 208.0+ bytes

    The ``dtype`` parameter can be used to specify the data types of the different
    columns:

    >>> df = get_df(
    ...     table="players",
    ...     keys=keys(player_id=["player_two", "player_four"]),
    ...         dtype={
    ...             "bonus_points": "Int8",
    ...             "last_play": "datetime64[ns, UTC]",
    ...             # "play_time": "timedelta64[ns]"  # See note below.
    ...         }
    ...     )
    >>> df.info()
    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 2 entries, 0 to 1
    Data columns (total 5 columns):
        #   Column        Non-Null Count  Dtype
       ---  ------        --------------  -----
        0   bonus_points  1 non-null      Int8
        1   player_id     2 non-null      object
        2   last_play     2 non-null      datetime64[ns, UTC]
        3   rating        2 non-null      float64
        4   play_time     2 non-null      object
    dtypes: Int8(1), datetime64[ns, UTC](1), float64(1), object(2)
    memory usage: 196.0+ bytes

    .. note:: Due to a
        `known bug in pandas <https://github.com/pandas-dev/pandas/issues/38509>`_,
        timedelta strings cannot currently be converted back to timedelta64 type via the
        ``dtype`` parameter. Use the ``pandas.to_timedelta`` function instead:

        >>> df.play_time = pd.to_timedelta(df.play_time)
        >>> df.info()
        <class 'pandas.core.frame.DataFrame'>
        RangeIndex: 2 entries, 0 to 1
        Data columns (total 5 columns):
            #   Column        Non-Null Count  Dtype
           ---  ------        --------------  -----
            0   bonus_points  1 non-null      Int8
            1   player_id     2 non-null      object
            2   last_play     2 non-null      datetime64[ns, UTC]
            3   rating        2 non-null      float64
            4   play_time     2 non-null      timedelta64[ns]
        dtypes: Int8(1), datetime64[ns, UTC](1), float64(1), object(1), timedelta64[ns](1)
        memory usage: 196.0+ bytes

    Omitting the ``keys`` parameter performs a scan of the table and returns all the
    items.

    >>> df = get_df(table="players")
    >>> print(df)
           bonus_points     player_id            last_play  rating        play_time
        0           4.0  player_three  2021-01-21 10:22:43     2.5  1 days 14:01:19
        1           NaN   player_four  2021-01-22 13:51:12     4.8  0 days 03:45:49
        2           3.0    player_one  2021-01-18 22:47:23     4.3  2 days 17:41:55
        3           1.0    player_two  2021-01-19 19:07:54     3.8  0 days 22:07:34
    """  # noqa: E501
    if keys is not None:
        items = get_items(keys=keys, table=table)
    else:
        items = get_all_items(table=table)

    return _to_df(items=items, dtype=dtype)


def keys(**kwargs):
    """Generate a list of key dictionaries from the partition key attribute name and a
    list of values. This can simplify the generation of keys to use with the ``get_df``
    function when only a partition key is used.

    Parameters
    ----------
    **kwargs
        A single keyword argument corresponding to the partition key name with a value
        corresponding to the list of key values to return.

    Returns
    -------
    list[dict]
        A list of key dictionaries.

    Examples
    --------

    Assuming we have a table with ``player_id`` as the partition key, we can generate
    the list of keys from the list of players:

    >>> key_list = keys(player_id=["player_two", "player_three", "player_four"])
    >>> print(key_list)
    [{'player_id': 'player_one'}, {'player_id': 'player_three'}, {'player_id': 'player_four'}]
    """  # noqa: E501
    if len(kwargs.keys()) > 1:
        raise ValueError("Only one key attribute (partition key) is supported.")

    k = list(kwargs.keys())[0]
    return [{k: v} for v in kwargs[k]]


def put_df(df, *, table):
    """Put rows of a dataframe as items into a table. If the item(s) do not exist in the
    table they are created, otherwise the existing items are replaced with the new ones.

    Parameters
    ----------
    df : pandas.DataFrame
        Dataframe of items to add/update in the table. The dataframe must, at a minimum,
        contain columns that correspond to the table's primary key attribute(s).

    table : str
        Name of the DynamoDB table.

    Examples
    --------
    Assume with have the following dataframe:

    >>> print(players_df)
          player_id           last_play       play_time  rating  bonus_points
    0    player_one 2021-01-18 22:47:23 2 days 17:41:55     4.3             3
    1    player_two 2021-01-19 19:07:54 0 days 22:07:34     3.8             1
    2  player_three 2021-01-21 10:22:43 1 days 14:01:19     2.5             4
    3   player_four 2021-01-22 13:51:12 0 days 03:45:49     4.8          <NA>

    The following will add or update the corresponding items in the table named
    ``players``:

    >>> put_df(players_df, table="players")
    """
    put_items(items=_to_items(df), table=table)


def _to_df(items, *, dtype=None):
    """Convert an item dictionary or list of item dictionaries into a pandas DataFrame.
    """
    if isinstance(items, dict):
        items = [items]

    df = pd.DataFrame(items)

    if dtype is not None:
        df = df.astype(dtype)

    return df


def _to_items(df):
    """Convert a pandas dataframe to a dictionary of items."""
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame")

    return df.to_dict("records")
