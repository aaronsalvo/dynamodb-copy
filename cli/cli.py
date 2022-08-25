import click
import re
import boto3
from pathlib import Path
from sys import exit
from os import environ
from progress.counter import Counter

from botocore.exceptions import ClientError, EndpointConnectionError


def get_profile_names():
    """Get a list of profile names from the AWS credentials file"""
    profile_names = []

    credentials_path = Path("~/.aws/credentials").expanduser()
    if 'AWS_SHARED_CREDENTIALS_FILE' in environ:
        credentials_path = Path(environ['AWS_SHARED_CREDENTIALS_FILE'])

    with open(credentials_path) as f:
        lines = f.readlines()
        for line in lines:
            if match := re.match(r'^\[([\s\w\-]+)\]$', line):
                profile_names.append(match.group(1))
    return profile_names


def validate_aws_credentials(profile_name, region_name):
    """
    Use the given AWS credentials to make a simple API call to ensure they are valid

    :param profile_name: the name of an AWS profile in the credentials file
    :param region_name: a valid AWS region name
    :return: an STS get-caller-identity response or exit with a non-zero exit code
    """
    token_error_msg = "ERROR: Security token issue: "
    try:
        sts_client = boto3.session.Session(profile_name=profile_name).client('sts', region_name=region_name)
        resp = sts_client.get_caller_identity()
        return resp['Account']
    except ClientError as e:
        if e.response['Error']['Code'] == "ExpiredToken":
            print(f"{token_error_msg} Credentials for profile '{profile_name}' are expired.")
            exit(1)
        if e.response['Error']['Code'] == "InvalidClientTokenId":
            print(f"{token_error_msg} Credentials for profile '{profile_name}' are invalid or do not have access to "
                  f"region '{region_name}'.")
            exit(1)
    except EndpointConnectionError as e:
        print(f"ERROR Invalid Region: The region '{region_name}' does not appear to be a valid AWS region.")
        exit(1)


def get_dynamodb_table(table_name, profile_name, region_name):
    """
    Create a boto3 DynamoDB Table resource for the given table

    :param table_name: the name of a DynamoDB table
    :param profile_name: the AWS credentials to use
    :param region_name: the region where the table exists
    :return: a boto3 DynamoDB Table resource object
    """

    dynamodb_client = boto3.Session(profile_name=profile_name, region_name=region_name).resource('dynamodb')
    table = dynamodb_client.Table(table_name)
    try:
        table.load()
    except dynamodb_client.meta.client.exceptions.ResourceNotFoundException:
        print(f"ERROR: count not find the table named '{table_name}'")
        exit(1)

    return table


def get_dynamodb_items(table):
    """
    Get all the items from a table.

    :param table: a boto3 Table Resource object
    :return: a list of dict with the items
    """

    items = []
    progress_msg = f"Downloading items from {table.table_name}: "
    with Counter(progress_msg) as bar:
        resp = table.scan()
        items += resp['Items']
        if 'LastEvaluatedKey' in resp:
            last_evaluated_key = resp['LastEvaluatedKey']
        else:
            last_evaluated_key = None
        bar.next(resp['Count'])

        while last_evaluated_key is not None:
            resp = table.scan(ExclusiveStartKey=last_evaluated_key)
            items += resp['Items']
            if 'LastEvaluatedKey' in resp:
                last_evaluated_key = resp['LastEvaluatedKey']
            else:
                last_evaluated_key = None
            bar.next(resp['Count'])

        bar.finish()

    return items


def write_items_to_dyanmodb_table(items, table):
    """
    Use the DynamoDB batch write API to write the provided items to a given table

    :param items: a list of items to write
    :param table: a boto3 DynamoDB Table Service Resource for the table to be written to
    :return:
    """
    item_count = len(items)
    with table.batch_writer() as batch:
        progress_msg = f"Writing {item_count:,} items to {table.table_name}: "
        with Counter(progress_msg) as bar:
            for item in items:
                batch.put_item(Item=item)
                bar.next()
        bar.finish()


@click.command()
@click.option('-st', '--src-table-name', prompt=f"Source table name")
@click.option('-sp', '--src-profile', prompt="Source AWS profile name", type=click.Choice(get_profile_names()))
@click.option('-sr', '--src-region', prompt="Source AWS region name")
@click.option('-dt', '--dest-table-name', prompt="Destination table name")
@click.option('-dp', '--dest-profile', prompt="Destination AWS profile name", type=click.Choice(get_profile_names()))
@click.option('-dr', '--dest-region', prompt="Destination AWS region name")
def run(src_table_name, src_profile, src_region, dest_table_name, dest_profile, dest_region):
    click.echo(f"src_table = {src_table_name}")
    click.echo(f"src_profile = {src_profile}")
    click.echo(f"src_region = {src_region}")
    click.echo(f"dest_table = {dest_table_name}")
    click.echo(f"dest_profile = {dest_profile}")
    click.echo(f"dest_region = {dest_region}")

    # Check that the profiles are valid
    validate_aws_credentials(src_profile, src_region)
    validate_aws_credentials(dest_profile, dest_region)

    # Load the tables
    src_table = get_dynamodb_table(src_table_name, src_profile, src_region)
    dest_table = get_dynamodb_table(dest_table_name, dest_profile, dest_region)

    # Get the source table's items
    src_items = get_dynamodb_items(src_table)

    # Write the items to the destination table
    write_items_to_dyanmodb_table(src_items, dest_table)


if __name__ == '__main__':
    run()
