"""Delete archives from an AWS Glacier vault using an inventory export.

Expects inventory.json (Glacier inventory job output) in the working directory.
"""

import json

import boto3

from scraper_place.config import CONFIG_S3

INPUT_FILE_NAME = 'inventory.json'
VAULT_NAME = 'scraper-place-prod'


def main():
    client = boto3.session.Session(
        aws_access_key_id=CONFIG_S3['aws_access_key_id'],
        aws_secret_access_key=CONFIG_S3['aws_secret_access_key'],
        region_name=CONFIG_S3['region_name'],
    ).client('glacier')

    with open(INPUT_FILE_NAME, encoding='utf-8') as inventory_file:
        archive_list = json.load(inventory_file)['ArchiveList']

    for i, archive in enumerate(archive_list):
        if i % 1000 == 0:
            print(i)
        client.delete_archive(
            accountId='-',
            vaultName=VAULT_NAME,
            archiveId=archive['ArchiveId'],
        )


if __name__ == '__main__':
    main()
