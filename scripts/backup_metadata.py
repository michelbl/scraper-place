
import pathlib
import datetime
import gzip

from bson import json_util
import boto3
from pymongo import MongoClient

from scraper_place.config import CONFIG_METADATA_BACKUP


if __name__ == '__main__':

    collection = MongoClient().place.dce
    data = list(collection.find({}))
    data_json = json_util.dumps(data)

    filename = 'metadata-{}.json.gz'.format(datetime.datetime.now().isoformat().split('T')[0])
    file_path = pathlib.Path(CONFIG_METADATA_BACKUP['repository']) / filename

    with gzip.open(file_path, 'wt', encoding='UTF-8') as f:
        f.write(data_json)

    s3_resource = boto3.session.Session(
        aws_access_key_id=CONFIG_METADATA_BACKUP['aws_access_key_id'],
        aws_secret_access_key=CONFIG_METADATA_BACKUP['aws_secret_access_key'],
        region_name=CONFIG_METADATA_BACKUP['region_name'],
    ).resource('s3')

    s3_resource.meta.client.upload_file(
        Filename=file_path.as_posix(),
        Bucket=CONFIG_METADATA_BACKUP['bucket_name'],
        Key=filename,
        ExtraArgs={'StorageClass': 'ONEZONE_IA'},
    )
