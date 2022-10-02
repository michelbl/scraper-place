"""extraction: Extract content using Apache Tika
"""

import json
import os
import urllib
import traceback
import time

from pymongo import MongoClient
import requests

from scraper_place.config import CONFIG_TIKA, CONFIG_ENV, STATE_CONTENT_EXTRACTING, STATE_CONTENT_EXTRACTION_KO, STATE_CONTENT_EXTRACTION_OK, STATE_GLACIER_OK, build_content_filepath, build_internal_filepath


def extract():
    """extract: Extract content from all DCEs
    """

    while True:
        client = MongoClient()
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

        extract_dce(dce_data=dce_data, tika_server_url=CONFIG_TIKA['tika_server_url'])

def extract_dce(dce_data, tika_server_url):
    """extract_dce: Extract the content of one DCE
    """

    try:
        annonce_id = dce_data['annonce_id']
        print('{} extracting content for DCE {}'.format(time.ctime(), annonce_id))

        content_list = []

        file_types = ['reglement', 'complement', 'avis', 'dce']
        filenames = [dce_data['filename_reglement'], dce_data['filename_complement'], dce_data['filename_avis'], dce_data['filename_dce']]

        for file_type, filename in zip(file_types, filenames):
            if not filename:
                continue

            internal_filepath = build_internal_filepath(annonce_id=annonce_id, original_filename=filename, file_type=file_type)
            if CONFIG_ENV['env'] != 'production':
                print('Debug: Extracting content of {}...'.format(internal_filepath))

            content, embedded_resource_paths = extract_file(file_path=internal_filepath, tika_server_url=tika_server_url)

            client = MongoClient()
            collection = client.place.dce
            collection.update_one(
                {'annonce_id': annonce_id},
                {'$set': {'embedded_filenames_{}'.format(file_type): embedded_resource_paths}}
            )
            client.close()

            content_list.append(content)

        content = '\n'.join(content_list)

        with open(build_content_filepath(annonce_id), 'w', encoding='utf-8') as f:
            f.write(content)

        client = MongoClient()
        collection = client.place.dce
        collection.update_one(
            {'annonce_id': annonce_id},
            {'$set': {'state': STATE_CONTENT_EXTRACTION_OK}}
        )
        client.close()

        if CONFIG_ENV['env'] != 'production':
            print('Debug: Extracted content from {}'.format(annonce_id))

    except Exception as exception:
        print("Warning: exception occured, aborting DCE ({}: {}) on {}".format(type(exception).__name__, exception, annonce_id))
        traceback.print_exc()

        client = MongoClient()
        collection = client.place.dce
        collection.update_one(
            {'annonce_id': annonce_id},
            {'$set': {'state': STATE_CONTENT_EXTRACTION_KO}}
        )
        client.close()

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
