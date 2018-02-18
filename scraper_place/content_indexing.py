"""content_indexing: Extract content using Apache Tika and ElasticSearch

Automatically spawns an EC2 instance and start a Tika server.

Make sure an ElasticSearch server is running.
"""

import json
import os
import urllib
import time
import traceback

import psycopg2
import requests
import boto3
import paramiko

from scraper_place.config import CONFIG_DATABASE, CONFIG_ELASTICSEARCH, CONFIG_AWS_EC2, CONFIG_ENV, STATE_GLACIER_OK, STATE_CONTENT_INDEXATION_OK, build_internal_filepath


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

    ec2_client, instance_id, ec2_ipv4, ssh_client = init_ec2()
    tika_server_url = 'http://{}:9998/'.format(ec2_ipv4)

    try:

        for dce_data in dce_data_list:
            annonce_id, org_acronym, filename_reglement, filename_complement, filename_avis, filename_dce = dce_data
            index_dce(annonce_id, org_acronym, filename_reglement, filename_complement, filename_avis, filename_dce, connection, cursor, tika_server_url)

    except Exception as exception:
        print("Warning: exception occured, terminating ({}: {})".format(type(exception).__name__, exception))
        traceback.print_exc()

    terminate_ec2(ec2_client, instance_id, ssh_client)

    cursor.close()
    connection.close()


def index_dce(annonce_id, org_acronym, filename_reglement, filename_complement, filename_avis, filename_dce, connection, cursor, tika_server_url):
    """index_dce(): Extract the content of one DCE and give it to ElasticSearch
    """

    content_list = []

    file_types = ['reglement', 'complement', 'avis', 'dce']
    filenames = [filename_reglement, filename_complement, filename_avis, filename_dce]

    try:
        for file_type, filename in zip(file_types, filenames):
            if not filename:
                continue

            internal_filepath = build_internal_filepath(annonce_id, org_acronym, filename, file_type)
            if CONFIG_ENV['env'] != 'production':
                print('Extracting content of {}...'.format(internal_filepath))

            content, embedded_resource_paths = extract_file(internal_filepath, tika_server_url)

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

    except Exception as exception:
        print("Warning: exception occured, aborting DCE ({}: {}) on {}-{}".format(type(exception).__name__, exception, annonce_id, org_acronym))
        traceback.print_exc()
        return

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
    print(url)
    print(headers)
    print(len(content))
    with open('content.tmp', 'w') as f:
        f.write(content)
    response = requests.put(url, headers=headers, json=data)
    assert response.status_code in {200, 201}, (response.status_code, response.text)


def extract_file(file_path, tika_server_url):
    url = urllib.parse.urljoin(tika_server_url, '/rmeta/text')
    headers = {
        'Accept': 'application/json',
    }
    with open(file_path, 'rb') as file_object:
        response = requests.put(url, headers=headers, data=file_object)
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


def init_ec2():
    ec2_client = boto3.client(
        'ec2',
        aws_access_key_id=CONFIG_AWS_EC2['aws_access_key_id'],
        aws_secret_access_key=CONFIG_AWS_EC2['aws_secret_access_key'],
        region_name=CONFIG_AWS_EC2['region_name'],
    )

    if CONFIG_ENV['env'] != 'production':
        print('Launching EC2 instance...')

    response = ec2_client.run_instances(
        BlockDeviceMappings=[
            {
                'DeviceName': '/dev/xvda',
                'Ebs': {
                    'DeleteOnTermination': True,
                    'VolumeSize': 8,
                    'VolumeType': 'gp2',
                },
            },
        ],
        ImageId='ami-5ce55321',
        InstanceType='t2.large',
        KeyName=CONFIG_AWS_EC2['key_name'],
        MaxCount=1,
        MinCount=1,
        SecurityGroupIds=[CONFIG_AWS_EC2['security_group']],
        #DryRun=True,
    )

    instance_id = response['Instances'][0]['InstanceId']

    waiter = ec2_client.get_waiter('instance_status_ok')
    waiter.wait(
        InstanceIds=[instance_id]
    )

    response = ec2_client.describe_instances(
        InstanceIds=[instance_id]
    )
    ec2_ipv4 = response['Reservations'][0]['Instances'][0]['PublicIpAddress']

    if CONFIG_ENV['env'] != 'production':
        print('Successfully launched instance {} with IPv4 {}'.format(instance_id, ec2_ipv4))

    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_key = paramiko.RSAKey.from_private_key_file(CONFIG_AWS_EC2['private_key'])
    ssh_client.connect(
        hostname=ec2_ipv4,
        username="ec2-user",
        pkey=ssh_key
    )

    ssh_channel = ssh_client.get_transport().open_session()
    ssh_channel.exec_command('wget http://apache.crihan.fr/dist/tika/tika-server-1.17.jar')
    assert ssh_channel.recv_exit_status() == 0

    ssh_channel = ssh_client.get_transport().open_session()
    ssh_channel.exec_command('sudo yum -y install java')
    assert ssh_channel.recv_exit_status() == 0

    ssh_channel = ssh_client.get_transport().open_session()
    ssh_channel.exec_command('java -Xmx7000m -jar tika-server-1.17.jar --host=* >tika-server.log 2>&1')

    time.sleep(10)  # give some time to Tika to start

    if CONFIG_ENV['env'] != 'production':
        print('Launched Tika server')

    return ec2_client, instance_id, ec2_ipv4, ssh_client


def terminate_ec2(ec2_client, instance_id, ssh_client):
    sftp_client = ssh_client.open_sftp()
    sftp_client.get('tika-server.log', os.path.join(CONFIG_AWS_EC2['logs_directory'], 'tika-server.log'))
    sftp_client.close()

    ssh_client.close()

    ec2_client.terminate_instances(
        InstanceIds=[instance_id],
    )

    if CONFIG_ENV['env'] != 'production':
        print('Terminated EC2 instance')
