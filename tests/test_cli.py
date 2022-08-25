import boto3
import pytest

from cli import get_profile_names, get_dynamodb_table, get_dynamodb_items, write_items_to_dyanmodb_table
from os import environ
from pathlib import Path


SRC_TABLE_NAME = 'TEST_SOURCE'
DEST_TABLE_NAME = 'TEST_DESTINATION'


@pytest.fixture
def test_tables():
    aws_profile_name = 'default'
    if 'AWS_PROFILE' in environ:
        aws_profile_name = environ['AWS_PROFILE']

    aws_region = 'us-east-1'
    if 'AWS_DEFAULT_REGION' in environ:
        aws_profile_name = environ['AWS_DEFAULT_REGION']

    dynamodb_client = boto3.Session(profile_name=aws_profile_name, region_name=aws_region).client('dynamodb')

    kwargs = {
        'AttributeDefinitions': [
           {
            'AttributeName': 'id',
            'AttributeType': 'S'
           }
           ],
        'KeySchema': [
            {
                'AttributeName': 'id',
                'KeyType': 'HASH'
            },
        ],
        'BillingMode': 'PAY_PER_REQUEST'
    }

    # Create test source and destination tables
    for table_name in [SRC_TABLE_NAME, DEST_TABLE_NAME]:
        dynamodb_client.create_table(**kwargs, TableName=table_name)
        waiter = dynamodb_client.get_waiter('table_exists')
        waiter.wait(TableName=SRC_TABLE_NAME)

    # Populate the source table with a few items
    for i in range(1, 3):
        dynamodb_client.put_item(
            TableName=SRC_TABLE_NAME,
            Item={'id': {'S': f"A{i}00"}, 'data': {'S': f"Data for item with ID A{i}00"}}
        )

    yield SRC_TABLE_NAME, DEST_TABLE_NAME

    # Delete the test source and destination tables
    for table_name in [SRC_TABLE_NAME, DEST_TABLE_NAME]:
        dynamodb_client.delete_table(TableName=table_name)
        waiter = dynamodb_client.get_waiter('table_not_exists')
        waiter.wait(TableName=SRC_TABLE_NAME)


class TestCli:
    def test_get_profile_names(self):
        environ['AWS_SHARED_CREDENTIALS_FILE'] = str(Path("resources/credentials"))

        expected_profile_names = ["profile_1",
                                  "profile_2",
                                  "profile-3",
                                  "profile 4", ]

        actual_profile_names = get_profile_names()

        assert len(actual_profile_names) == 4
        assert expected_profile_names == actual_profile_names

    def test_get_dynamodb_table(self, test_tables):
        bad_table_name = 'FAKE_TABLE_NAME'

        actual_table_name, _ = test_tables

        with pytest.raises(SystemExit):
            get_dynamodb_table(bad_table_name, 'default', 'us-east-1')

        actual_table = get_dynamodb_table(actual_table_name, 'default', 'us-east-1')

        assert actual_table.creation_date_time is not None

    def test_get_dynamodb_items(self, test_tables):
        test_table_name, _ = test_tables
        table = get_dynamodb_table(test_table_name, ':wq', 'us-east-1')

        items = get_dynamodb_items(table)

        assert items is not None

    def test_write_items_to_dyanmodb_table(self, test_tables):
        test_src_table_name, test_dest_table_name = test_tables
        src_table = get_dynamodb_table(test_src_table_name, 'default', 'us-east-1')
        dest_table = get_dynamodb_table(test_dest_table_name, 'default', 'us-east-1')

        src_items = get_dynamodb_items(src_table)

        write_items_to_dyanmodb_table(src_items, dest_table)

        dest_items = get_dynamodb_items(dest_table)

        assert dest_items == src_items

