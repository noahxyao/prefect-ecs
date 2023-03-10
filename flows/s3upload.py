from prefect import task, flow
from prefect import get_run_logger
from flows.healthcheck import healthcheck  # to show how subflows can be packaged and imported

import boto3
from botocore.exceptions import ClientError
import logging
import os


@task
def upload_file(file_name: str, bucket: str, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    """

    logger = get_run_logger()
    logger.info(f"Starting S3 Upload for bucket {bucket} ...")

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        logger.error(e)
        logger.info(f"S3 Upload failed for file {file_name}")
    logger.info(f"S3 Upload succeeded for file {file_name}")


@flow
def s3upload(
    file_name: str = "utilities/img.jpeg", 
    bucket: str = "datadrivers-img-bucket", 
    object_name: str = "img.jpeg"
    ):
    # upload image file task
    upload_file(file_name, bucket, object_name)
    # nesting second healthcheck flow
    healthcheck()


if __name__ == "__main__":
    s3upload(bucket_arn="s3")
