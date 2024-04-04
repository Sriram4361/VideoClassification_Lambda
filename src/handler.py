# __copyright__   = "Copyright 2024, VISA Lab"
# __license__     = "MIT"

from boto3 import client as boto3_client
from video_splitting_cmdline import video_splitting_cmdline
from urllib.parse import unquote_plus
import os
s3_client = boto3_client('s3')
destination_bucket = '1230041516-stage-1'


def handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        
        print("key", key)
        
        download_path = '/tmp/'+os.path.basename(key)

        file_name = os.path.basename(key)
        print("file_name", download_path)
        file_name_without_extension = os.path.splitext(file_name)[0]
        
        s3_client.download_file(bucket, key, download_path)
        print("file downloaded")
        out_dir = video_splitting_cmdline(download_path)
        os.remove(download_path)
        
        upload_directory_to_s3(out_dir, file_name_without_extension)
        
        os.system(f"rm -rf {out_dir}")


def upload_directory_to_s3(directory_path, file_name_without_extension):
    for root, _, files in os.walk(directory_path):
        for file in files:
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, directory_path)
            s3_key = f"{file_name_without_extension}/{relative_path}"

            print(f"Uploading {local_path} to {destination_bucket}/{s3_key}")
            s3_client.upload_file(local_path, destination_bucket, s3_key)
