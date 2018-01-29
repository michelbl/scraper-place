"""content_indexing: Extract content using Apache Tika and ElasticSearch

Make sure that a tika server is running and accepting file urls:
`java -jar tika-server-1.17.jar -enableUnsecureFeatures -enableFileUrl`

Make sure an ElasticSearch server is running.
"""

import json
import os
import urllib

import psycopg2
import requests

from scraper_place.config import CONFIG_TIKA, CONFIG_DATABASE, CONFIG_ELASTICSEARCH, CONFIG_ENV, STATE_GLACIER_OK, STATE_CONTENT_INDEXATION_OK, build_internal_filepath


def index():
    """index(): Extract content from all DCE and index it in ElasticSearch.
    """

    # Open connection
    connection = psycopg2.connect(
        dbname=CONFIG_DATABASE['name'],
        user=CONFIG_DATABASE['username'],
        password=CONFIG_DATABASE['password'],
    )
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT annonce_id, org_acronym,
            filename_reglement, filename_complement, filename_avis, filename_dce
        FROM dce
        WHERE state = %s
        ;""",
        (STATE_GLACIER_OK, )
    )

    dce_data_list = cursor.fetchall()
    for dce_data in dce_data_list:
        annonce_id, org_acronym, filename_reglement, filename_complement, filename_avis, filename_dce = dce_data
        index_dce(annonce_id, org_acronym, filename_reglement, filename_complement, filename_avis, filename_dce, connection, cursor)

    cursor.close()
    connection.close()


def index_dce(annonce_id, org_acronym, filename_reglement, filename_complement, filename_avis, filename_dce, connection, cursor):
    """index_dce(): Extract the content of one DCE and give it to ElasticSearch
    """

    content_list = []

    file_types = ['reglement', 'complement', 'avis', 'dce']
    filenames = [filename_reglement, filename_complement, filename_avis, filename_dce]

    for file_type, filename in zip(file_types, filenames):
        if not filename:
            continue

        internal_filepath = build_internal_filepath(annonce_id, org_acronym, filename, file_type)
        if CONFIG_ENV['env'] != 'production':
            print('Extracting content of {}...'.format(internal_filepath))

        content, embedded_resource_paths = extract_file(internal_filepath)

        psql_request_template = """
            UPDATE dce
            SET embedded_filenames_{} = %s
            WHERE annonce_id = %s AND org_acronym = %s
            ;""".format(file_type)
        cursor.execute(
            psql_request_template,
            (embedded_resource_paths, annonce_id, org_acronym)
        )
        connection.commit()

        content_list.append(content)

    content = '\n'.join(content_list)

    feed_elastisearch(annonce_id, org_acronym, content)

    cursor.execute(
        """
        UPDATE dce
        SET state = %s
        WHERE annonce_id = %s AND org_acronym = %s
        ;""",
        (STATE_CONTENT_INDEXATION_OK, annonce_id, org_acronym)
    )
    connection.commit()

    if CONFIG_ENV['env'] != 'production':
        print('Extracted content from {}-{}'.format(annonce_id, org_acronym))


def feed_elastisearch(annonce_id, org_acronym, content):
    url = urllib.parse.urljoin(
        CONFIG_ELASTICSEARCH['elasticsearch_server_url'],
        '/'.join([
            CONFIG_ELASTICSEARCH['index_name'],
            CONFIG_ELASTICSEARCH['document_type'],
            '{}-{}'.format(annonce_id, org_acronym),
        ])
    )

    headers = {
        'Content-Type': 'application/json',
    }
    data = {
        "content" : content,
    }
    response = requests.put(url, headers=headers, json=data)
    assert response.status_code == 200, (response.status_code, response.text)


def extract_file(file_path):
    url = urllib.parse.urljoin(CONFIG_TIKA['tika_server_url'], '/rmeta/text')
    headers = {
        'fileUrl': 'file://{}'.format(file_path),
        'Accept': 'application/json',
    }
    response = requests.put(url, headers=headers)

    tika_result = json.loads(response.content)  # better than r.text that takes hours to compute

    content_list, embedded_resource_paths = filter_content(tika_result)

    content = '\n'.join(content_list)

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
