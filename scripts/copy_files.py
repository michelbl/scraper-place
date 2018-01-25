import datetime
import re
import configparser
import os
import zipfile
import glob
import shutil

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

vestibule_directory = config['file_storage']['vestibule_directory']
tmp_directory = config['file_storage']['tmp_directory']
public_directory = config['file_storage']['public_directory']
archive_directory = config['file_storage']['archive_directory']
file_storage_badfiles_dir = config['file_storage']['badfiles_dir']
len_tmp_dir = len(tmp_directory) + 1

regex_basename = r'^([^-]+)-([^-]+)-([^-]+)$'


def process_file(source_filename):
    basename, ext = os.path.splitext(source_filename)
    source_filepath = os.path.join(vestibule_directory, source_filename)

    
    if ext == '.zip':
        os.mkdir(tmp_directory)
        try:
            process_archive(source_filename)
        except ZipFileException as e:
            print('Warning : bad files {} ({})'.format(source_filename, e))
            badfiles_filepath = os.path.join(file_storage_badfiles_dir, source_filename)
            os.rename(source_filepath, badfiles_filepath)
        else:
            archive_file(source_filename)
        shutil.rmtree(tmp_directory, ignore_errors=True)
    else:        
        archive_file(source_filename)


def archive_file(source_filename):
    basename, ext = os.path.splitext(source_filename)
    source_filepath = os.path.join(vestibule_directory, source_filename)
    archive_filepath = os.path.join(archive_directory, source_filename)

    copy_archive(basename, ext)

    os.rename(source_filepath, archive_filepath)


class ZipFileException(Exception):
    pass


def process_archive(source_filename):
    basename, ext = os.path.splitext(source_filename)
    source_filepath = os.path.join(vestibule_directory, source_filename)

    try:
        with zipfile.ZipFile(source_filepath, "r") as zip_ref:
            zip_ref.extractall(tmp_directory)
    except (zipfile.BadZipFile, UnicodeDecodeError) as e:
        raise ZipFileException(str(e))


    new_filepaths = glob.glob(tmp_directory + '/**', recursive=True)
    for new_filepath in new_filepaths:
        assert new_filepath[:len_tmp_dir] == tmp_directory + '/'
        variable_part = new_filepath[len_tmp_dir:]

        if os.path.islink(new_filepath):
            raise ValueError('Dangerous ! {} from {} is a link !'.format(new_filepath, source_filename))
        elif os.path.isdir(new_filepath):
            create_dir(basename, variable_part)
        elif os.path.isfile(new_filepath):
            copy_file(basename, variable_part)
        else:
            raise ValueError('Unknown nature of {} from {} !'.format(new_filepath, source_filename))


def create_dir(basename, variable_part):
    target_path = os.path.join(public_directory, basename, variable_part)
    os.makedirs(target_path, exist_ok=True)
    
    annonce_id, org_acronym, document_type = re.match(regex_basename, basename).groups()
    data_tuple = (
        annonce_id, org_acronym, document_type,
        True, '', 'dir', variable_part,
        )
    cursor.execute("""
        SELECT * from files WHERE (
            annonce_id = %s AND
            org_acronym = %s AND
            document_type = %s AND
            is_in_archive = %s AND
            ext = %s AND
            node_type = %s AND
            variable_part = %s
        )""",
            data_tuple
        )
    if not cursor.fetchall():
        cursor.execute("""
            INSERT INTO files (
                annonce_id, org_acronym, document_type,
                is_in_archive, ext, node_type, variable_part
                )
                VALUES (
                %s, %s, %s,
                %s, %s, %s, %s
            )""",
                data_tuple
            )
        connection.commit()

def copy_file(basename, variable_part):
    create_dir(basename, os.path.dirname(variable_part))

    source_filepath = os.path.join(tmp_directory, variable_part)
    target_filepath = os.path.join(public_directory, basename, variable_part)
    shutil.copyfile(source_filepath, target_filepath)

    annonce_id, org_acronym, document_type = re.match(regex_basename, basename).groups()
    cursor.execute("""
        INSERT INTO files (
            annonce_id, org_acronym, document_type,
            is_in_archive, ext, node_type, variable_part
            )
            VALUES (
            %s, %s, %s,
            %s, %s, %s, %s
        )""",
        (
            annonce_id, org_acronym, document_type,
            True, '', 'file', variable_part,
            )
        )
    connection.commit()

def copy_archive(basename, ext):
    source_filename = basename + ext
    source_filepath = os.path.join(vestibule_directory, source_filename)
    target_filepath = os.path.join(public_directory, source_filename)
    shutil.copyfile(source_filepath, target_filepath)

    annonce_id, org_acronym, document_type = re.match(regex_basename, basename).groups()
    cursor.execute("""
        INSERT INTO files (
            annonce_id, org_acronym, document_type,
            is_in_archive, ext, node_type, variable_part
            )
            VALUES (
            %s, %s, %s,
            %s, %s, %s, %s
           )""",
        (
            annonce_id, org_acronym, document_type,
            False, ext, '', '',
            )
        )
    connection.commit()

        
# Open connection
connection = psycopg2.connect(dbname=database_name, user=database_username, password=database_password)
cursor = connection.cursor()

source_filenames = os.listdir(vestibule_directory)

for source_filename in source_filenames:
    process_file(source_filename)

cursor.close()
connection.close()
