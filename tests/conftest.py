import base64
import os

import boto3
from moto import mock_dynamodb2
import pytest
from test_data import large_table_items
from test_data import test_df

from dynamo_pandas.transactions import put_item


@pytest.fixture()
def aws_credentials():
    """Dummy AWS credentials to use with the moto mocks."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture()
def ddb_client(aws_credentials):
    """Fixture to mock the dynamodb client using moto."""
    with mock_dynamodb2():
        yield boto3.client("dynamodb")


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
