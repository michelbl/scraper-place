"""config: Provide config, constants and helper functions
"""
import configparser
import os


BASE_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
#base_dir = os.getcwd()  # notebook


CONFIG = configparser.ConfigParser()
CONFIG_PATH = os.path.join(BASE_DIR, 'config.ini')
CONFIG.read(CONFIG_PATH)

CONFIG_ENV = dict(CONFIG.items('env'))
CONFIG_DATABASE = dict(CONFIG.items('database'))
CONFIG_FILE_STORAGE = dict(CONFIG.items('file_storage'))
CONFIG_AWS_GLACIER = dict(CONFIG.items('aws_glacier'))
CONFIG_TIKA = dict(CONFIG.items('tika'))
CONFIG_ELASTICSEARCH = dict(CONFIG.items('elasticsearch'))


# Other values are not guaranteed to work (especially localhost='127.0.0.1')
assert CONFIG_DATABASE['host'] == 'localhost'
assert CONFIG_DATABASE['port'] == '1234'


# Possible values for the processing state
STATE_FETCH_OK = 'fetch_ok'
STATE_GLACIER_OK = 'glacier_ok'
STATE_CONTENT_INDEXATION_OK = 'content_indexation_ok'


def build_internal_filepath(annonce_id, org_acronym, original_filename, file_type):
    """build_internal_filepath(): build the filename used for storage

    file_type: One of 'reglement', 'complement', 'avis', 'dce'
    """

    extention = os.path.splitext(original_filename)[1]
    internal_filename = '{}-{}-{}{}'.format(annonce_id, org_acronym, file_type, extention)
    internal_filepath = os.path.join(CONFIG_FILE_STORAGE['public_directory'], internal_filename)

    return internal_filepath
