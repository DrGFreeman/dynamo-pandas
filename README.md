[![unit-tests-linux](https://github.com/drgfreeman/dynamo-pandas/actions/workflows/linux-checks.yml/badge.svg)](https://github.com/DrGFreeman/dynamo-pandas/actions/workflows/linux-checks.yml)
[![unit-tests-windows](https://github.com/drgfreeman/dynamo-pandas/actions/workflows/windows-checks.yml/badge.svg)](https://github.com/DrGFreeman/dynamo-pandas/actions/workflows/windows-checks.yml)
[![Documentation Status](https://readthedocs.org/projects/dynamo-pandas/badge/?version=latest)](https://dynamo-pandas.readthedocs.io/en/latest/?badge=latest)

# dynamo-pandas
Make working with pandas data and AWS DynamoDB easy.

## Motivation
This package aims a making the transfer of data between pandas dataframes and DynamoDB as simple as possible. To meet this goal, the package offers two key features:
1. Automatic conversion of pandas data types to DynamoDB supported data types.
1. A simple, high level interface to *put* data from a dataframe into a DynamoDB table and *get* all or selected items from a table into a dataframe.


## Documentation

The project's documentation is available at https://dynamo-pandas.readthedocs.io/.


## Requirements
* `python>=3.7`
* `boto3`
* `pandas>=1`

## Installation

```
python -m pip install dynamo-pandas
```
## Example Usage

Consider the pandas DataFrame below.


```
>>> print(players_df)

      player_id           last_play       play_time  rating  bonus_points
0    player_one 2021-01-18 22:47:23 2 days 17:41:55     4.3             3
1    player_two 2021-01-19 19:07:54 0 days 22:07:34     3.8             1
2  player_three 2021-01-21 10:22:43 1 days 14:01:19     2.5             4
3   player_four 2021-01-22 13:51:12 0 days 03:45:49     4.8          <NA>
```

The columns of the dataframe use different data types, some of which are not natively supported by DynamoDB, like numpy.datetime64, timedelta64 and pandas' nullable integers.


```
>>> players_df.info()

<class 'pandas.core.frame.DataFrame'>
RangeIndex: 4 entries, 0 to 3
Data columns (total 5 columns):
    #   Column        Non-Null Count  Dtype          
---  ------        --------------  -----          
    0   player_id     4 non-null      object         
    1   last_play     4 non-null      datetime64[ns] 
    2   play_time     4 non-null      timedelta64[ns]
    3   rating        4 non-null      float64        
    4   bonus_points  3 non-null      Int8           
dtypes: Int8(1), datetime64[ns](1), float64(1), object(1), timedelta64[ns](1)
memory usage: 264.0+ bytes
```

Storing the rows of this dataframe to DynamoDB requires multiple data type conversions.

```
>>> from dynamo_pandas import put_df, get_df, keys
```

The `put_df` function adds or updates the rows of a dataframe into the specified table, taking care of the required type conversions (the table must be already created and the primary key column(s) be present in the dataframe).

```
>>> put_df(players_df, table="players")
```

The `get_df` function retrieves the items matching the speficied key(s) from the table into a dataframe.


```
>>> df = get_df(table="players", keys=[{"player_id": "player_three"}, {"player_id": "player_one"}])
>>> print(df)

   bonus_points     player_id            last_play  rating        play_time
0             4  player_three  2021-01-21 10:22:43     2.5  1 days 14:01:19
1             3    player_one  2021-01-18 22:47:23     4.3  2 days 17:41:55
```

In the case where only a partition key is used, the `keys` function simplifies the generation of the keys list.


```
>>> df = get_df(table="players", keys=keys(player_id=["player_two", "player_four"]))
>>> print(df)

   bonus_points    player_id            last_play  rating        play_time
0           1.0   player_two  2021-01-19 19:07:54     3.8  0 days 22:07:34
1           NaN  player_four  2021-01-22 13:51:12     4.8  0 days 03:45:49
```

The data types returned by the `get_df` function are basic types and no automatic type conversion is attempted.


```
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
```

The `dtype` parameter of the `get_df` function allows specifying the desired data types.

```
>>> df = get_df(
...     table="players",
...     keys=keys(player_id=["player_two", "player_four"]),
...         dtype={
...             "bonus_points": "Int8",
...             "last_play": "datetime64[ns, UTC]",
...             # "play_time": "timedelta64[ns]"  # See note below.
...         }
...     )
```

**Note**: Due to a known bug in pandas, timedelta strings cannot currently be converted back to Timedelta type via this parameter (ref. https://github.com/pandas-dev/pandas/issues/38509). Use the pandas.to_timedelta function instead:


```
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
```

Omitting the `keys` parameter performs a scan of the table and returns all the items.


```
>>> df = get_df(table="players")
>>> print(df)

       bonus_points     player_id            last_play  rating        play_time
    0           4.0  player_three  2021-01-21 10:22:43     2.5  1 days 14:01:19
    1           NaN   player_four  2021-01-22 13:51:12     4.8  0 days 03:45:49
    2           3.0    player_one  2021-01-18 22:47:23     4.3  2 days 17:41:55
    3           1.0    player_two  2021-01-19 19:07:54     3.8  0 days 22:07:34
```

## License

Released under the terms of the [MIT License](LICENSE).