import os

import boto3
from moto import mock_dynamodb2
import pytest


@pytest.fixture()
def aws_credentials():
    """Dummy AWS credentials to use with the moto mocks."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture()
def ddb_client(aws_credentials):
    """Fixture to mock the dynamodb client using moto."""
    with mock_dynamodb2():
        yield boto3.client("dynamodb")
