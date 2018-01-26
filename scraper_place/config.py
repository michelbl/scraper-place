"""config: Read config
"""
import configparser
import os


BASE_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
#base_dir = os.getcwd()  # notebook


CONFIG = configparser.ConfigParser()
CONFIG_PATH = os.path.join(BASE_DIR, 'config.ini')
CONFIG.read(CONFIG_PATH)

CONFIG_DATABASE = dict(BASE_DIR.items('database'))
CONFIG_FILE_STORAGE = dict(BASE_DIR.items('file_storage'))
CONFIG_AWS_GLACIER = dict(BASE_DIR.items('aws_glacier'))


# Other values are not guaranteed to work (especially localhost='127.0.0.1')
assert CONFIG_DATABASE['host'] == 'localhost'
assert CONFIG_DATABASE['port'] == '1234'
