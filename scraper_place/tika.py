"""tika: Extract content using Apache Tika

Make sure that a tika server is running and accepting file urls:
`java -jar tika-server-1.17.jar -enableUnsecureFeatures -enableFileUrl`
"""

import json
import os
import urllib

import requests

from scraper_place.config import CONFIG_TIKA


def process_file(file_path):
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
