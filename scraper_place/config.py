"""config: Provide config, constants and helper functions
"""
import configparser
import os
import logging
import logging.handlers


BASE_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
#base_dir = os.getcwd()  # notebook


CONFIG = configparser.ConfigParser()
CONFIG_PATH = os.path.join(BASE_DIR, 'config.ini')
CONFIG.read(CONFIG_PATH)

CONFIG_ENV = dict(CONFIG.items('env'))
CONFIG_FILE_STORAGE = dict(CONFIG.items('file_storage'))
CONFIG_S3 = dict(CONFIG.items('s3'))
CONFIG_TIKA = dict(CONFIG.items('tika'))
CONFIG_ELASTICSEARCH = dict(CONFIG.items('elasticsearch'))


# Possible values for the processing state
STATE_FETCH_OK = 'fetch_ok'
STATE_GLACIER_OK = 'glacier_ok'
STATE_GLACIER_KO = 'glacier_ko'
STATE_CONTENT_EXTRACTING = 'extracting'
STATE_CONTENT_EXTRACTION_OK = 'extraction_ok'
STATE_CONTENT_EXTRACTION_KO = 'extraction_ko'
STATE_CONTENT_INDEXATION_OK = 'indexation_ok'


def build_internal_filepath(annonce_id, original_filename, file_type):
    """build_internal_filepath: build the filename used for storage

    file_type: One of 'reglement', 'complement', 'avis', 'dce'
    """

    extention = os.path.splitext(original_filename)[1]
    internal_filename = '{}-{}{}'.format(annonce_id, file_type, extention)
    internal_filepath = os.path.join(CONFIG_FILE_STORAGE['public_directory'], internal_filename)

    return internal_filepath

def build_extract_filepath(annonce_id):
    return os.path.join(CONFIG_FILE_STORAGE['extract_output_dir'], '{}.txt.gz'.format(annonce_id))

def configure_logging():
    logger = logging.getLogger()
    formatter = logging.Formatter(
        '%(asctime)s %(levelname)s: %(message)s'
    )
    logger.setLevel(logging.DEBUG)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.WARNING)
    stream_handler.setFormatter(formatter)

    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename=CONFIG_ENV['log_path'], when='midnight', backupCount=0)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

configure_logging()
