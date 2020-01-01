"""glacier: save the DCEs to AWS Glacier
"""

import os

from pymongo import MongoClient
import boto3

from scraper_place.config import CONFIG_AWS_GLACIER, STATE_FETCH_OK, STATE_GLACIER_OK, STATE_GLACIER_KO, CONFIG_ENV, build_internal_filepath


def save():
    """save(): Save all the DCEs to AWS Glacier and keep their archive id in the database.
    """

    client = MongoClient()
    collection = client.place.dce

    s3_resource = boto3.session.Session(
        aws_access_key_id=CONFIG_AWS_GLACIER['aws_access_key_id'],
        aws_secret_access_key=CONFIG_AWS_GLACIER['aws_secret_access_key'],
        region_name=CONFIG_AWS_GLACIER['region_name'],
    ).resource('s3')

    cursor = collection.find({'state': STATE_FETCH_OK})
    for dce_data in cursor:
        save_dce(dce_data=dce_data, s3_resource=s3_resource, collection=collection)

    client.close()


def save_dce(dce_data, s3_resource, collection):
    """save_dce(): Save one DCE to AWS Glacier
    """
    annonce_id = dce_data['annonce_id']
    file_types = ['reglement', 'complement', 'avis', 'dce']
    filenames = [dce_data['filename_reglement'], dce_data['filename_complement'], dce_data['filename_avis'], dce_data['filename_dce']]

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
            return

    for file_type, filename in zip(file_types, filenames):
        if not filename:
            continue

        internal_filepath = build_internal_filepath(annonce_id=annonce_id, original_filename=filename, file_type=file_type)
        internal_filename = os.path.basename(internal_filepath)
        if CONFIG_ENV['env'] != 'production':
            print('Debug: Saving {} on AWS S3 Glacier Deep Archive...'.format(internal_filepath))
        s3_resource.meta.client.upload_file(
            Filename=internal_filepath,
            Bucket=CONFIG_AWS_GLACIER['bucket_name'],
            Key=internal_filename,
            ExtraArgs={'StorageClass': 'DEEP_ARCHIVE'}
        )

    collection.update_one(
        {'annonce_id': annonce_id},
        {'$set': {'state': STATE_GLACIER_OK}},
    )

    if CONFIG_ENV['env'] != 'production':
        print('Debug: Saved {} on AWS Glavier'.format(annonce_id))
