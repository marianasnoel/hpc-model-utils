from os import makedirs
from os.path import join

import boto3


def check_items_in_bucket(
    bucket_name: str,
    remote_prefix: str,
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
) -> list[str]:
    s3 = boto3.resource(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    bucket = s3.Bucket(bucket_name)
    objects: list[str] = []
    try:
        objects = [
            obj.key for obj in bucket.objects.filter(Prefix=remote_prefix)
        ]
    except Exception:
        return []
    return objects


def upload_file_to_bucket(
    local_filepath: str,
    destination_bucket: str,
    remote_filepath: str,
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
):
    s3 = boto3.resource(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    result = s3.Bucket(destination_bucket).upload_file(
        local_filepath, remote_filepath
    )

    return result


def download_bucket_items(
    bucket_name: str,
    prefixes: list[str],
    destination: str,
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
) -> list[str]:
    s3 = boto3.resource(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    bucket = s3.Bucket(bucket_name)
    makedirs(destination, exist_ok=True)

    success_downloaded_paths = []
    for p in prefixes:
        filename = p.split("/")[-1]
        filepath = join(destination, filename)
        bucket.download_file(p, filepath)
        success_downloaded_paths.append(filepath)
    return success_downloaded_paths
