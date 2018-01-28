"""glacier: save the DCEs to AWS Glacier
"""

import psycopg2
import boto3

from scraper_place.config import CONFIG_DATABASE, CONFIG_AWS_GLACIER, STATE_FETCH_OK, STATE_GLACIER_OK, build_internal_filepath


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
        save_dce(annonce_id, org_acronym, intitule, filename_reglement, filename_complement, filename_avis, filename_dce, cursor, glacier_client)

    cursor.close()
    connection.close()


def save_dce(annonce_id, org_acronym, intitule, filename_reglement, filename_complement, filename_avis, filename_dce, cursor, glacier_client):
    """save_dce(): Save one DCE to AWS Glacier
    """
    file_types = ['reglement', 'complement', 'avis', 'dce']
    filenames = [filename_reglement, filename_complement, filename_avis, filename_dce]

    for file_type, filename in zip(file_types, filenames):
        if not filename:
            continue

        archive_description = '{}-{} {} ({}) {}'.format(annonce_id, org_acronym, file_type, filename, intitule)
        
        internal_filepath = build_internal_filepath(annonce_id, org_acronym, filename, file_type)
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
            FROM dce
            WHERE annonce_id = %s AND org_acronym = %s
            ;""".format(file_type)
        cursor.execute(
            psql_request_template,
            (annonce_id, org_acronym, archive_id)
        )

    cursor.execute(
        """
        UPDATE dce
        SET state = %s
        FROM dce
        WHERE annonce_id = %s AND org_acronym = %s
        ;""",
        (STATE_GLACIER_OK,  org_acronym, archive_id)
    )
