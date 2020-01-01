"""glacier: save the DCEs to AWS Glacier
"""

import os

from pymongo import MongoClient
import boto3
from unidecode import unidecode

from scraper_place.config import CONFIG_AWS_GLACIER, STATE_FETCH_OK, STATE_GLACIER_OK, STATE_GLACIER_KO, CONFIG_ENV, build_internal_filepath


def save():
    """save(): Save all the DCEs to AWS Glacier and keep their archive id in the database.
    """

    client = MongoClient()
    collection = client.place.dce

    glacier_client = boto3.client(
        'glacier',
        aws_access_key_id=CONFIG_AWS_GLACIER['aws_access_key_id'],
        aws_secret_access_key=CONFIG_AWS_GLACIER['aws_secret_access_key'],
        region_name=CONFIG_AWS_GLACIER['region_name'],
    )

    cursor = collection.find({'state': STATE_FETCH_OK})
    for dce_data in cursor:
        save_dce(dce_data=dce_data, glacier_client=glacier_client)

    client.close()


def save_dce(dce_data, glacier_client):
    """save_dce(): Save one DCE to AWS Glacier
    """
    annonce_id = dce_data['annonce_id']
    file_types = ['reglement', 'complement', 'avis', 'dce']
    filenames = [dce_data['filename_reglement'], dce_data['filename_complement'], dce_data['filename_avis'], dce_data['filename_dce']]

    client = MongoClient()
    collection = client.place.dce

    # Checks if the file is not too large to be uploaded using boto3 (max 4Go)
    # If a file is that large, we probably don't want to backup nor index it.
    for file_type, filename in zip(file_types, filenames):
        if not filename:
            continue

        internal_filepath = build_internal_filepath(annonce_id=annonce_id, original_filename=filename, file_type=file_type)

        file_size = os.path.getsize(internal_filepath)
        if file_size >= 4294967296:
            print('Warning: {} is too large to be saved on AWS Glacier'.format(internal_filepath))

            collection.update_one(
                {'annonce_id': annonce_id},
                {'$set': {'state': STATE_GLACIER_KO}},
            )
            client.close()
            return

    for file_type, filename in zip(file_types, filenames):
        if not filename:
            continue

        archive_description = '{} {} ({}) {}'.format(annonce_id, file_type, filename, dce_data['intitule'])
        archive_description = unidecode(archive_description)
        archive_description = archive_description[:1023]
        archive_description = archive_description.replace('\t', '    ')

        internal_filepath = build_internal_filepath(annonce_id=annonce_id, original_filename=filename, file_type=file_type)
        if CONFIG_ENV['env'] != 'production':
            print('Debug: Saving {} on AWS Glacier...'.format(internal_filepath))
            print(archive_description)
        with open(internal_filepath, 'rb') as file_object:
            response = glacier_client.upload_archive(
                vaultName=CONFIG_AWS_GLACIER['vault_name'],
                archiveDescription=archive_description,
                body=file_object,
            )
        assert response['ResponseMetadata']['HTTPStatusCode'] == 201, archive_description
        archive_id = response['archiveId']

        collection.update_one(
            {'annonce_id': annonce_id},
            {'$set': {'glacier_id_{}'.format(file_type): archive_id}},
        )

    collection.update_one(
        {'annonce_id': annonce_id},
        {'$set': {'state': STATE_GLACIER_OK}},
    )

    if CONFIG_ENV['env'] != 'production':
        print('Debug: Saved {} on AWS Glavier'.format(annonce_id))

    client.close()
