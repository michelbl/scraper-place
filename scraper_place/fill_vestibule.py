"""fill_vestibule: Fetch the new DCE from https://www.marches-publics.gouv.fr/.

Store metadatata on database and store the archives on the vestibule.
"""

import datetime
import re
import os

import psycopg2

from scraper_place.config import CONFIG_DATABASE, CONFIG_FILE_STORAGE, CONFIG_ENV, STATE_FETCH_OK
from scraper_place import fetch


VESTIBULE = CONFIG_FILE_STORAGE['vestibule_directory']


def fill_vestibule():

    # Open connection
    connection = psycopg2.connect(
        dbname=CONFIG_DATABASE['name'],
        user=CONFIG_DATABASE['username'],
        password=CONFIG_DATABASE['password'],
    )
    cursor = connection.cursor()

    if CONFIG_ENV['production']:
        nb_pages = 0
    else:
        nb_pages = 1

    links = fetch.fetch_current_annonces(nb_pages=1)

    nb_processed = 0
    for link in links:
        nb_processed += process_link(link, connection, cursor)
    print("Info: Processed {} DCE".format(nb_processed))    

    cursor.close()
    connection.close()


def process_link(link, connection, cursor):
    """
    process_link : Download data and store it in database.
    Return the number of stored DCE (0 or 1).
    """
    annonce_id, org_acronym = re.match(fetch.link_regex, link).groups()

    # abort if the DCE is already processed
    cursor.execute("SELECT annonce_id, org_acronym FROM dce WHERE annonce_id = %s AND org_acronym = %s;", (annonce_id, org_acronym))
    results = cursor.fetchall()
    if results:
        return 0

    try:
        (
            annonce_id, org_acronym, links_boamp,
            reference, intitule, objet, reglement_ref,
            filename_reglement, reglement, filename_complement,
            complement, filename_avis, avis, filename_dce, dce) = fetch.fetch_data(link)
    except Exception as e:
        print("Warning: exception occured ({}) : {}".format(e, link))
        return 0

    now = datetime.datetime.now()

    if reglement:
        extention = os.path.splitext(filename_reglement)[1]
        filename = '{}-{}-reglement{}'.format(annonce_id, org_acronym, extention)
        filepath = os.path.join(VESTIBULE, filename)
        with open(filepath, 'wb') as f:
            f.write(reglement)

    if avis:
        extention = os.path.splitext(filename_avis)[1]
        filename = '{}-{}-avis{}'.format(annonce_id, org_acronym, extention)
        filepath = os.path.join(VESTIBULE, filename)
        with open(filepath, 'wb') as f:
            f.write(avis)

    if complement:
        extention = os.path.splitext(filename_complement)[1]
        filename = '{}-{}-complement{}'.format(annonce_id, org_acronym, extention)
        filepath = os.path.join(VESTIBULE, filename)
        with open(filepath, 'wb') as f:
            f.write(complement)

    if dce:
        extention = os.path.splitext(filename_dce)[1]
        filename = '{}-{}-dce{}'.format(annonce_id, org_acronym, extention)
        filepath = os.path.join(VESTIBULE, filename)
        with open(filepath, 'wb') as f:
            f.write(dce)

    cursor.execute(
        """
        INSERT INTO dce (
            annonce_id, org_acronym, links_boamp,
            reference, intitule, objet,
            reglement_ref, filename_reglement, filename_complement, filename_avis, filename_dce,
            fetch_datetime,
            state
            )
            VALUES (
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s,
            %s
           )""",
        (
            annonce_id, org_acronym, links_boamp,
            reference, intitule, objet,
            reglement_ref, filename_reglement, filename_complement, filename_avis, filename_dce,
            now,
            STATE_FETCH_OK
            )
        )
    connection.commit()
    return 1
