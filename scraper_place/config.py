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
CONFIG_FILE_STORAGE = dict(CONFIG.items('file_storage'))
CONFIG_METADATA_BACKUP = dict(CONFIG.items('metadata_backup'))
CONFIG_AWS_GLACIER = dict(CONFIG.items('aws_glacier'))
CONFIG_TIKA = dict(CONFIG.items('tika'))
CONFIG_ELASTICSEARCH = dict(CONFIG.items('elasticsearch'))
CONFIG_AWS_EC2 = dict(CONFIG.items('aws_ec2'))


# Possible values for the processing state
STATE_FETCH_OK = 'fetch_ok'
STATE_GLACIER_OK = 'glacier_ok'
STATE_GLACIER_KO = 'glacier_ko'
STATE_CONTENT_INDEXATION_OK = 'content_indexing_ok'
STATE_CONTENT_INDEXATION_KO = 'content_indexing_ko'
STATE_INDEXING = 'indexing'


def build_internal_filepath(annonce_id, original_filename, file_type):
    """build_internal_filepath: build the filename used for storage

    file_type: One of 'reglement', 'complement', 'avis', 'dce'
    """

    extention = os.path.splitext(original_filename)[1]
    internal_filename = '{}-{}{}'.format(annonce_id, file_type, extention)
    internal_filepath = os.path.join(CONFIG_FILE_STORAGE['public_directory'], internal_filename)

    return internal_filepath
