"""glacier: save the DCEs to AWS Glacier
"""

import psycopg2
import boto3
from unidecode import unidecode

from scraper_place.config import CONFIG_DATABASE, CONFIG_AWS_GLACIER, STATE_FETCH_OK, STATE_GLACIER_OK, CONFIG_ENV, build_internal_filepath


def save():
    """save(): Save all the DCEs to AWS Glacier and keep their archive id in the database.
    """

    # Open connection
    connection = psycopg2.connect(
        dbname=CONFIG_DATABASE['name'],
        user=CONFIG_DATABASE['username'],
        password=CONFIG_DATABASE['password'],
    )
    cursor = connection.cursor()

    glacier_client = boto3.client(
        'glacier',
        aws_access_key_id=CONFIG_AWS_GLACIER['aws_access_key_id'],
        aws_secret_access_key=CONFIG_AWS_GLACIER['aws_secret_access_key'],
        region_name=CONFIG_AWS_GLACIER['region_name'],
    )

    cursor.execute(
        """
        SELECT annonce_id, org_acronym, intitule,
            filename_reglement, filename_complement, filename_avis, filename_dce
        FROM dce
        WHERE state = %s
        ;""",
        (STATE_FETCH_OK, )
    )

    dce_data_list = cursor.fetchall()
    for dce_data in dce_data_list:
        annonce_id, org_acronym, intitule, filename_reglement, filename_complement, filename_avis, filename_dce = dce_data
        save_dce(annonce_id, org_acronym, intitule, filename_reglement, filename_complement, filename_avis, filename_dce, connection, cursor, glacier_client)

    cursor.close()
    connection.close()


def save_dce(annonce_id, org_acronym, intitule, filename_reglement, filename_complement, filename_avis, filename_dce, connection, cursor, glacier_client):
    """save_dce(): Save one DCE to AWS Glacier
    """
    file_types = ['reglement', 'complement', 'avis', 'dce']
    filenames = [filename_reglement, filename_complement, filename_avis, filename_dce]

    for file_type, filename in zip(file_types, filenames):
        if not filename:
            continue

        archive_description = '{}-{} {} ({}) {}'.format(annonce_id, org_acronym, file_type, filename, intitule)
        archive_description = unidecode(archive_description)
        archive_description = archive_description[:1023]
        archive_description = archive_description.replace('\t', '    ')

        internal_filepath = build_internal_filepath(annonce_id, org_acronym, filename, file_type)
        if CONFIG_ENV['env'] != 'production':
            print('Debug: Saving {} on AWS Glavier...'.format(internal_filepath))
            print(archive_description)
        with open(internal_filepath, 'rb') as file_object:
            response = glacier_client.upload_archive(
                vaultName=CONFIG_AWS_GLACIER['vault_name'],
                archiveDescription=archive_description,
                body=file_object,
            )
        assert response['ResponseMetadata']['HTTPStatusCode'] == 201, archive_description
        archive_id = response['archiveId']
        psql_request_template = """
            UPDATE dce
            SET glacier_id_{} = %s
            WHERE annonce_id = %s AND org_acronym = %s
            ;""".format(file_type)
        cursor.execute(
            psql_request_template,
            (archive_id, annonce_id, org_acronym)
        )
        connection.commit()

    cursor.execute(
        """
        UPDATE dce
        SET state = %s
        WHERE annonce_id = %s AND org_acronym = %s
        ;""",
        (STATE_GLACIER_OK, annonce_id, org_acronym)
    )
    connection.commit()

    if CONFIG_ENV['env'] != 'production':
        print('Debug: Saved {}-{} on AWS Glavier'.format(annonce_id, org_acronym))
