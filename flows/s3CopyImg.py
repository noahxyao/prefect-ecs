from prefect import task, flow
from prefect import get_run_logger
from flows.healthcheck import healthcheck  # to show how subflows can be packaged and imported

import boto3
from botocore.exceptions import ClientError
import logging
import os


@task
def upload_file(source_bucket: str, source_bucket_key: str, destination_bucket: str, destination_bucket_key: str):
    """Copy a file between 2 S3 buckets

    :param source_bucket: source bucket where file is copied from
    :param source_bucket_key: path to file in source bucket
    :param destination_bucket: destination bucket where file is copied to
    :param destination_bucket_key: path to file in destination bucket
    """

    logger = get_run_logger()
    logger.info(f"Starting S3 copy from bucket {source_bucket} to bucket {destination_bucket} ...")

    # Copy the file
    s3 = boto3.resource('s3')
    copy_source = {
        'Bucket': source_bucket,
        'Key': source_bucket_key
    }
    try:
        s3.meta.client.copy(copy_source, destination_bucket, destination_bucket_key)
    except ClientError as e:
        logging.error(e)
        logger.error(e)
        logger.info(f"S3 Upload failed for file {source_bucket_key}")
    logger.info(f"S3 Upload succeeded for file {source_bucket_key}")


@flow
def s3upload(
    source_bucket: str = "datadrivers-noah-test",
    source_bucket_key: str = "prod/utilities/img.jpeg",
    destination_bucket: str = "datadrivers-img-bucket", 
    destination_bucket_key: str = "images/", 
    ):
    # upload image file task
    upload_file(source_bucket, source_bucket_key, destination_bucket, destination_bucket_key)
    # nesting second healthcheck flow
    healthcheck()


if __name__ == "__main__":
    s3upload(bucket_arn="s3")
