import datetime
import re
import configparser
import os

import psycopg2

import fetch

base_dir = os.path.dirname(os.path.realpath(__file__))
#base_dir = os.getcwd()  # notebook
base_dir


# Read config

config = configparser.ConfigParser()
config_path = os.path.join(base_dir, 'config.ini')
config.read(config_path)
database_host = config['database']['host']
database_port = config['database']['port']
database_name = config['database']['name']
database_username = config['database']['username']
database_password = config['database']['password']
assert database_host == 'localhost'
assert database_port == '1234'

file_storage_dir = config['file_storage']['dir']


# Open connection
connection = psycopg2.connect(dbname=database_name, user=database_username, password=database_password)
cursor = connection.cursor()


def process_link(link, connection, cursor):
    annonce_id, org_acronym = re.match(fetch.link_regex, link).groups()
    
    cursor.execute("SELECT annonce_id, org_acronym FROM dce WHERE annonce_id = %s AND org_acronym = %s;", (annonce_id, org_acronym))
    results = cursor.fetchall()
    
    if results:
        return
    
    print("Info: Processing {}".format(link))
    
    try:
        (annonce_id, org_acronym, links_boamp,
            reference, intitule, objet, reglement_ref,
            filename_reglement, reglement, filename_complement,
            complement, filename_avis, avis, filename_dce, dce) = fetch.fetch_data(link)
    except Exception as e:
        print("Warning: exception occured ({})".format(e))
        return
    
    now = datetime.datetime.now()
    
    if reglement:
        extention = os.path.splitext(filename_reglement)[1]
        filename = '{}-{}-reglement{}'.format(annonce_id, org_acronym, extention)
        filepath = os.path.join(file_storage_dir, filename)
        with open(filepath, 'wb') as f:
            f.write(reglement)
    
    if avis:
        extention = os.path.splitext(filename_avis)[1]
        filename = '{}-{}-avis{}'.format(annonce_id, org_acronym, extention)
        filepath = os.path.join(file_storage_dir, filename)
        with open(filepath, 'wb') as f:
            f.write(avis)
    
    if complement:
        extention = os.path.splitext(filename_complement)[1]
        filename = '{}-{}-complement{}'.format(annonce_id, org_acronym, extention)
        filepath = os.path.join(file_storage_dir, filename)
        with open(filepath, 'wb') as f:
            f.write(complement)
    
    if dce:
        extention = os.path.splitext(filename_dce)[1]
        filename = '{}-{}-dce{}'.format(annonce_id, org_acronym, extention)
        filepath = os.path.join(file_storage_dir, filename)
        with open(filepath, 'wb') as f:
            f.write(dce)
    
    cursor.execute("""
        INSERT INTO dce (
            annonce_id, org_acronym, links_boamp,
            reference, intitule, objet,
            reglement_ref, filename_reglement, filename_complement, filename_avis, filename_dce,
            fetch_datetime
            )
            VALUES (
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s
           )""",
        (
            annonce_id, org_acronym, links_boamp,
            reference, intitule, objet,
            reglement_ref, filename_reglement, filename_complement, filename_avis, filename_dce,
            now,
            )
        )
    connection.commit()


links = fetch.fetch_current_annonces(nb_pages=0)  # Set to 1 for a developpement setup


for link in links:
    process_link(link, connection, cursor)

cursor.close()
connection.close()
