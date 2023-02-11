"""indexation: Index content using ElasticSearch

Make sure an ElasticSearch server is running.
"""

import json
import os
import urllib
import traceback

from pymongo import MongoClient
import requests
from elasticsearch import Elasticsearch

from scraper_place.config import CONFIG_ELASTICSEARCH, CONFIG_ENV, STATE_CONTENT_EXTRACTION_OK, STATE_CONTENT_INDEXATION_OK, build_content_filepath


def index():
    """index(): Extract content from all DCE and index it in ElasticSearch.
    """

    while True:
        client = MongoClient()
        collection = client.place.dce
        dce_list = list(collection.find({'state': STATE_CONTENT_EXTRACTION_OK}).limit(1))

        if not dce_list:
            client.close()
            break

        dce_data = dce_list[0]
        client.close()

        index_dce(dce_data=dce_data)

def index_dce(dce_data):
    """index_dce(): Index the content of one DCE using ElasticSearch
    """

    annonce_id = dce_data['annonce_id']

    with open(build_content_filepath(annonce_id), 'r', encoding='utf-8') as f:
        content = f.read()

    data = {
        'annonce_id': dce_data['annonce_id'],
        'org_acronym': dce_data['org_acronym'],
        'links_boamp': dce_data['links_boamp'],
        'reference': dce_data['reference'],
        'intitule': dce_data['intitule'],
        'objet': dce_data['objet'],
        'reglement_ref': dce_data['reglement_ref'],
        'filename_reglement': dce_data['filename_reglement'],
        'filename_complement': dce_data['filename_complement'],
        'filename_avis': dce_data['filename_avis'],
        'filename_dce': dce_data['filename_dce'],
        'fetch_datetime': dce_data['fetch_datetime'],
        'file_size_reglement': dce_data['file_size_reglement'],
        'file_size_complement': dce_data['file_size_complement'],
        'file_size_avis': dce_data['file_size_avis'],
        'file_size_dce': dce_data['file_size_dce'],
        'embedded_filenames_reglement': dce_data.get('embedded_filenames_reglement'),
        'embedded_filenames_complement': dce_data.get('embedded_filenames_complement'),
        'embedded_filenames_avis': dce_data.get('embedded_filenames_avis'),
        'embedded_filenames_dce': dce_data.get('embedded_filenames_dce'),
        'content': content
    }

    es_client = Elasticsearch([CONFIG_ELASTICSEARCH['elasticsearch_server_url']])
    es_client.create(
        index=CONFIG_ELASTICSEARCH['index_name'],
        id='{}'.format(dce_data['annonce_id']),
        body=data,
        timeout='60s',
        request_timeout=60,
    )

    client = MongoClient()
    collection = client.place.dce
    collection.update_one(
        {'annonce_id': annonce_id},
        {'$set': {'state': STATE_CONTENT_INDEXATION_OK}}
    )
    client.close()
