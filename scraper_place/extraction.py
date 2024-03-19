"""extraction: Extract content using Apache Tika
"""

import json
import os
import urllib
import traceback
import time
import logging
import gzip

from pymongo import MongoClient
import requests
import boto3

from scraper_place.config import CONFIG_ENV, CONFIG_MONGODB, CONFIG_S3, CONFIG_TIKA, STATE_CONTENT_EXTRACTING, STATE_CONTENT_EXTRACTION_KO, STATE_CONTENT_EXTRACTION_OK, STATE_GLACIER_OK, build_extract_filepath, build_internal_filepath


def extract():
    """extract: Extract content from all DCEs
    """

    s3_resource = boto3.session.Session(
        aws_access_key_id=CONFIG_S3['aws_access_key_id'],
        aws_secret_access_key=CONFIG_S3['aws_secret_access_key'],
        region_name=CONFIG_S3['region_name'],
    ).resource('s3', endpoint_url=CONFIG_S3['endpoint_url'])

    while True:
        client = MongoClient(CONFIG_MONGODB['mongo_uri'])
        collection = client.place.dce
        dce_list = list(collection.find({'state': STATE_GLACIER_OK}).limit(1))

        if not dce_list:
            client.close()
            break

        dce_data = dce_list[0]
        collection.update_one(
            {'annonce_id': dce_data['annonce_id']},
            {'$set': {'state': STATE_CONTENT_EXTRACTING}}
        )
        client.close()

        extract_dce(
            dce_data=dce_data,
            tika_server_url=CONFIG_TIKA['tika_server_url'],
            s3_resource=s3_resource,
        )

def extract_dce(dce_data, tika_server_url, s3_resource):
    """extract_dce: Extract the content of one DCE
    """

    try:
        annonce_id = dce_data['annonce_id']
        logging.debug('{} extracting content for DCE {}'.format(time.ctime(), annonce_id))

        content_list = []

        file_types = ['reglement', 'complement', 'avis', 'dce']
        filenames = [dce_data['filename_reglement'], dce_data['filename_complement'], dce_data['filename_avis'], dce_data['filename_dce']]

        for file_type, filename in zip(file_types, filenames):
            if not filename:
                continue

            internal_filepath = build_internal_filepath(annonce_id=annonce_id, original_filename=filename, file_type=file_type)
            logging.debug('Extracting content of {}...'.format(internal_filepath))

            content, embedded_resource_paths = extract_file(file_path=internal_filepath, tika_server_url=tika_server_url)

            client = MongoClient(CONFIG_MONGODB['mongo_uri'])
            collection = client.place.dce
            collection.update_one(
                {'annonce_id': annonce_id},
                {'$set': {'embedded_filenames_{}'.format(file_type): embedded_resource_paths}}
            )
            client.close()

            content_list.append(content)

        content = '\n'.join(content_list)

        extract_filepath = build_extract_filepath(annonce_id)
        extract_filename = os.path.basename(extract_filepath)
        with gzip.open(extract_filepath, 'wt', encoding='UTF-8') as f:
            f.write(content)

        s3_resource.meta.client.upload_file(
            Filename=extract_filepath,
            Bucket=CONFIG_S3['extract_backup_bucket_name'],
            Key=extract_filename,
            ExtraArgs={'StorageClass': CONFIG_S3['storage_class_one_zone_ia']}
        )

        client = MongoClient(CONFIG_MONGODB['mongo_uri'])
        collection = client.place.dce
        collection.update_one(
            {'annonce_id': annonce_id},
            {'$set': {'state': STATE_CONTENT_EXTRACTION_OK}}
        )
        client.close()

        logging.debug('Extracted content from {}'.format(annonce_id))

    except Exception as exception:
        logging.warning("Exception of type {} occured, aborting DCE {}".format(type(exception).__name__, annonce_id))
        logging.debug("Exception details: {}".format(exception))
        logging.debug(traceback.format_exc())

        client = MongoClient(CONFIG_MONGODB['mongo_uri'])
        collection = client.place.dce
        collection.update_one(
            {'annonce_id': annonce_id},
            {'$set': {'state': STATE_CONTENT_EXTRACTION_KO}}
        )
        client.close()
        time.sleep(5)  # Give some time to the Tika server to restart

def extract_file(file_path, tika_server_url):
    url = urllib.parse.urljoin(tika_server_url, '/rmeta/text')
    headers = {
        'Accept': 'application/json',
    }
    with open(file_path, 'rb') as file_object:
        response = requests.put(url, headers=headers, data=file_object, timeout=3600)
    assert response.status_code == 200, (response.status_code, response.text)

    tika_result = json.loads(response.content)  # better than r.text that takes hours to compute

    content_list, embedded_resource_paths = filter_content(tika_result)

    content = '\n'.join(content_list)

    # See ipython notebook stats_elasticsearch
    if len(content) > 10000000:
        content = content[:5000000] + content[-5000000:]

    embedded_resource_paths = sorted(embedded_resource_paths)

    return content, embedded_resource_paths


def filter_content(tika_result):
    content_list = []
    embedded_resource_paths = []
    for file_data in tika_result:
        if 'X-TIKA:embedded_resource_path' in file_data:
            embedded_resource_paths.append(file_data['X-TIKA:embedded_resource_path'])
        elif 'resourceName' in file_data:
            embedded_resource_paths.append(file_data['resourceName'])

        if 'resourceName' in file_data:
            filename = file_data['resourceName']
            if is_unwanted_type(filename):
                continue

        if 'X-TIKA:content' in file_data:  # can also be a image PDF
            file_content = file_data['X-TIKA:content']
            content_list.append(file_content)

    return content_list, embedded_resource_paths


def is_unwanted_type(filename):
    _, file_extension = os.path.splitext(filename)
    if file_extension.lower() in {
            '.pdf',
            '.doc', '.xls', '.ppt',
            '.docx', '.xlsx', '.pptx',
            '.odt', '.ods', '.odp'
            '.html',
            '.txt', '.rtf',
    }:
        return False
    return True

if __name__ == '__main__':
    extract()
