from boto3.dynamodb.types import NULL
from boto3.dynamodb.types import NUMBER
from boto3.dynamodb.types import STRING
from boto3.dynamodb.types import TypeDeserializer as DDBTypeDeserializer
from boto3.dynamodb.types import TypeSerializer as DDBTypeSerializer
import numpy as np
import pandas as pd


class TypeSerializer(DDBTypeSerializer):
    """An extension of the `boto3.dynamodb.types.TypeSerializer
    <https://boto3.amazonaws.com/v1/documentation/api/latest/_modules/boto3/dynamodb/types.html>`_
    class with support for pandas data types.

    Differences in type conversion from the parent class are the following::

        Python                                  DynamoDB
        ------                                  --------
        numpy.nan, pandas.NA                    {'NULL': True}
        int, numpy.integer                      {'N': str(value)}
        float, numpy.floating                   {'N': str(value)}
        pandas nullable Int64(32, 16, 8)        {'N': str(value)}
        pandas.Timestamp                        {'S': str(value)}
        pandas.Timedelta                        {'S': str(value)}
    """

    def _get_dynamodb_type(self, value):

        # Pandas NA values
        if value is pd.NA or value is pd.NaT:
            dynamodb_type = NULL

        elif isinstance(value, (pd.Timestamp, pd.Timedelta)):
            dynamodb_type = STRING

        elif isinstance(value, (int, np.integer)):
            dynamodb_type = NUMBER

        elif isinstance(value, (float, np.floating)):
            if np.isnan(value):
                dynamodb_type = NULL
            else:
                dynamodb_type = NUMBER

        else:
            dynamodb_type = super()._get_dynamodb_type(value)

        return dynamodb_type

    def _serialize_s(self, value):
        return str(value)

    def _serialize_n(self, value):
        if value % 1 == 0:
            v = int(value)
        else:
            v = str(value)
        return super()._serialize_n(v)


class TypeDeserializer(DDBTypeDeserializer):
    """An extension of the `boto3.dynamodb.types.TypeDeserializer
    <https://boto3.amazonaws.com/v1/documentation/api/latest/_modules/boto3/dynamodb/types.html>`_
    class with conversion of numbers to float and int types instead of Decimal.

    Differences in type conversion from the parent class are the following::

        DynamoDB                                Python
        --------                                ------
        {'N': str(value)}                       int/float
    """

    def _deserialize_n(self, value):
        v = float(value)
        if v % 1 == 0:
            return int(v)
        else:
            return v
