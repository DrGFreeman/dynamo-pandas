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


def keys(**kwargs):
    """Generate a list of key dictionaries from the partition key attribute name and a
    list of values."""
    if len(kwargs.keys()) > 1:
        raise ValueError("Only one key attribute (partition key) is supported.")

    k = list(kwargs.keys())[0]
    return [{k: v} for v in kwargs[k]]


def get_item(*, key, table):
    """Get a single item from a table."""
    table = boto3.resource("dynamodb").Table(table)

    item = table.get_item(Key=key).get("Item")

    return _deserialize(item)


def get_items(*, keys, table):
    """Get a multiple items from a table."""
    resource = boto3.resource("dynamodb")

    def request(keys, table=table):
        return {table: {"Keys": keys}}

    def _get_items(keys, table=table):
        response = resource.batch_get_item(RequestItems=request(keys))
        items = response["Responses"][table]

        while response["UnprocessedKeys"] != {}:
            response = resource.batch_get_item(RequestItems=request(keys))
            items.extend(response["Responses"][table])

        return items

    key_batches = _batches(keys, batch_size=100)

    items = []
    for key_batch in key_batches:
        items.extend(_get_items(key_batch))

    return _deserialize(items)


def get_all_items(*, table):
    """Get all the items in a table."""
    table = boto3.resource("dynamodb").Table(table)

    response = table.scan()
    items = response["Items"]

    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        items.extend(response["Items"])

    return _deserialize(items)


def put_item(*, item, table, return_response=False):
    """Add or update an item in a table."""
    if not isinstance(item, dict):
        raise TypeError("item must be a non-empty dictionary")

    client = boto3.client("dynamodb")

    response = client.put_item(TableName=table, Item=ts.serialize(item)["M"])

    if return_response:
        return response
