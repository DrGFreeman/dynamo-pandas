import boto3

from dynamo_pandas.serde import TypeDeserializer
from dynamo_pandas.serde import TypeSerializer

ts = TypeSerializer()
td = TypeDeserializer()


def _deserialize(items):
    """Convert dictionaries to DynamoDB format and back."""
    return td.deserialize(ts.serialize(items))


def _batches(items, batch_size):
    """Split an iterable in batches."""
    items = list(items)
    start = 0
    while start < len(items):
        end = min(start + batch_size, len(items))
        yield items[start:end]
        start += batch_size


def get_item(*, key, table):
    """Get a single item from a table.

    Parameters
    ----------
    key : dict
        Key of the item to get.

    table : str
        Name of the DynamoDB table.

    Returns
    -------
    dict, None
        A dictionary representing the item's attributes. None if the key does not exist
        in the table.

    Examples
    --------

    >>> item = get_item(key={"player_id": "player_two"}, table="players")
    >>> print(item)
    {'bonus_points': 1,
     'player_id': 'player_two',
     'last_play': '2021-01-19 19:07:54',
     'rating': 3.8,
     'play_time': '0 days 22:07:34'}
    """
    table = boto3.resource("dynamodb").Table(table)

    item = table.get_item(Key=key).get("Item")

    return _deserialize(item)


def get_items(*, keys, table):
    """Get multiple items from a table.

    Parameters
    ----------
    keys : list[dict]
        List of key dictionaries of the items to get.

    table : str
        Name of the DynamoDB table.

    Returns
    -------
    list[dict]
        List of dictionaties where each dictionary represents an item's attributes.
        Only items for which the key exists in the table are returned.

    Examples
    --------

    >>> items = get_items(
    ...     keys=[
    ...         {"player_id": "player_two"},
    ...         {"player_id": "player_one"},
    ...         {"player_id": "player_five"}, # Not in the table
    ...     ],
    ...     table="players"
    ... )
    >>> print(items)
    [{'bonus_points': 3, 'player_id': 'player_one', 'last_play': '2021-01-18 22:47:23', 'rating': 4.3, 'play_time': '2 days 17:41:55'},
     {'bonus_points': 1, 'player_id': 'player_two', 'last_play': '2021-01-19 19:07:54', 'rating': 3.8, 'play_time': '0 days 22:07:34'}]
    """  # noqa: E501

    def _request(keys, table=table):
        return {table: {"Keys": keys}}

    def _get_items(keys, table=table):
        response = resource.batch_get_item(RequestItems=_request(keys))
        items = response["Responses"][table]

        while response["UnprocessedKeys"] != {}:
            response = resource.batch_get_item(RequestItems=_request(keys))
            items.extend(response["Responses"][table])

        return items

    resource = boto3.resource("dynamodb")

    key_batches = _batches(keys, batch_size=100)

    items = []
    for key_batch in key_batches:
        items.extend(_get_items(key_batch))

    return _deserialize(items)


def get_all_items(*, table):
    """Get all the items in a table.

    This function performs a scan of the table.

    Parameters
    ----------
    table : str
        Name of the DynamoDB table.

    Returns
    -------
    list[dict]
        List of dictionaties where each dictionary represents an item's attributes.

    Examples
    --------

    >>> items = get_all_items(table="players")
    >>> print(items)
    [{'bonus_points': 4, 'player_id': 'player_three', 'last_play': '2021-01-21 10:22:43', 'rating': 2.5, 'play_time': '1 days 14:01:19'},
     {'bonus_points': None, 'player_id': 'player_four', 'last_play': '2021-01-22 13:51:12', 'rating': 4.8, 'play_time': '0 days 03:45:49'},
     {'bonus_points': 3, 'player_id': 'player_one', 'last_play': '2021-01-18 22:47:23', 'rating': 4.3, 'play_time': '2 days 17:41:55'},
     {'bonus_points': 1, 'player_id': 'player_two', 'last_play': '2021-01-19 19:07:54', 'rating': 3.8, 'play_time': '0 days 22:07:34'}]
    """  # noqa: E501
    table = boto3.resource("dynamodb").Table(table)

    response = table.scan()
    items = response["Items"]

    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        items.extend(response["Items"])

    return _deserialize(items)


def put_item(*, item, table, return_response=False):
    """Add or update an item in a table. If the item does not exist in the table it is
    created, otherwise the existing item is replaced with the new one.

    Item can use supported numpy or pandas data types.

    Parameters
    ----------
    item : dict
        A dictionary representing the item's attributes. The item must include the
        table's primary key attributes.

    table : str
        Name of the DynamoDB table.

    return_response : bool
        If True, the response from the boto3 API call will be returned.

    Returns
    -------
    None, dict
        None if ``return_response`` is False, the boto3 API call response if True.

    Examples
    --------

    >>> print(item)
    {'player_id': 'player_three',
     'bonus_points': 4,
     'last_play': Timestamp('2021-01-21 10:22:43+0000', tz='UTC'),
     'rating': 2.5,
     'play_time': Timedelta('1 days 14:01:19')}
    >>> response = put_item(item=item, table="players", return_response=True)
    >>> print(response["ResponseMetadata"]["HTTPStatusCode"])
    200
    """
    if not isinstance(item, dict):
        raise TypeError("item must be a non-empty dictionary")

    client = boto3.client("dynamodb")

    response = client.put_item(TableName=table, Item=ts.serialize(item)["M"])

    if return_response:
        return response


def put_items(*, items, table):
    """Add or update multiple items in a table. If the item(s) do not exist in the
    table they are created, otherwise the existing items are replaced with the new ones.

    Items can use supported numpy or pandas data types.

    Parameters
    ----------
    items : list[dict]
        List of dictionaties where each dictionary represents an item's attributes.

    table : str
        Name of the DynamoDB table.

    Examples
    --------

    >>> pprint(items)
    [{'bonus_points': 4,
      'last_play': Timestamp('2021-01-21 10:22:43+0000', tz='UTC'),
      'play_time': Timedelta('1 days 14:01:19'),
      'player_id': 'player_three',
      'rating': 2.5},
     {'bonus_points': <NA>,
      'last_play': Timestamp('2021-01-22 13:51:12+0000', tz='UTC'),
      'play_time': Timedelta('0 days 03:45:49'),
      'player_id': 'player_four',
      'rating': 4.8}]
    >>> put_items(items=items, table="players)
    """
    if not isinstance(items, list):
        raise TypeError("items must be a list of non-empty dictionaries")

    def _put_items(items, table=table):
        response = client.batch_write_item(
            RequestItems={table: [{"PutRequest": {"Item": item}} for item in items]}
        )
        if response["UnprocessedItems"] != {}:
            return response["UprocessedItems"][table]
        else:
            return []

    client = boto3.client("dynamodb")

    items_to_process = [i["M"] for i in ts.serialize(items)["L"]]

    batch_size = 25
    while len(items_to_process) > 0:
        batch_items = items_to_process[:batch_size]
        items_to_process = items_to_process[batch_size:]

        unprocessed_items = _put_items(batch_items)

        if len(unprocessed_items) > batch_size // 2:
            batch_size = batch_size // 2 + 1

        # Put unprocessed items at back of queue.
        items_to_process.extend(unprocessed_items)
