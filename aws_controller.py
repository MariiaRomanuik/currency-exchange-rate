import logging
import boto3
from botocore.exceptions import ClientError
import awswrangler as wr
import os


def create_bucket(bucketName, awsRegion):
    s3 = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    buckets_name = []
    for bucket in s3_resource.buckets.all():
        buckets_name.append(bucket.name)
    if bucketName not in buckets_name:
        s3.create_bucket(Bucket=bucketName, CreateBucketConfiguration={'LocationConstraint': awsRegion})
        print("Bucket created")
        return True
    else:
        print("Bucket with such name already exist")
        return False


def upload_file_to_s3(fileName, bucket, objectName=None):
    # If S3 object_name was not specified, use fileName
    if objectName is None:
        objectName = os.path.basename(fileName)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(fileName, bucket, objectName)
        os.remove(fileName)
        print(fileName, "successfully removed")
    except ClientError as e:
        logging.error(e)
        return False
    return True


def is_in_s3(fileName, bucketName):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucketName)
    objs = list(bucket.objects.filter(Prefix=fileName))
    if any([w.key == fileName for w in objs]):
        print("File",  fileName, "exists in", bucketName, "bucket!")
        return True
    else:
        print("File", fileName, "doesn't exists in", bucketName, "bucket!")
        return False


def read_csv_from_s3_as_df(path):
    df = wr.s3.read_csv(path=path)
    return df


# if __name__ == "__main__":
#     file_name = "currency.csv"
#     csv_file_name = f'data/{file_name}'
#     bucket_name = 's3-all-data'
#     s3_path_to_file = f's3://{bucket_name}/{file_name}'
#     bucket_name = 's3-all-data'
#     region = 'us-east-2'
#     # print(create_bucket(bucket_name, region))
#     # print(upload_file_to_s3(file_name, bucket_name))
#     if is_in_s3(file_name, bucket_name):
#         print(read_csv_from_s3_as_df(s3_path_to_file))

