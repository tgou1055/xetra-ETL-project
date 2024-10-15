"""
Connector and methods accessing S3 buckets on AWS
"""
import os
import logging
import boto3

class S3BucketConnector():
    """
    Class for interacting with S3 buckets
    """

    def __init__(self, s3_access_key: str, s3_secret_key: str, endpoint_url: str, bucket: str):
        """
        Constructor for S3BucketConnector

        :param access_key: for accessing S3 bucket
        :param secret_key: for accessing S3 bucket
        :param endpoint_url: endpoint url in S3
        :param bucket: S3 bucket name
        """
        self._logger = logging.getLogger(__name__)
        self.endpoint_url = endpoint_url
        self.session = boto3.Session(aws_access_key_id=os.environ[s3_access_key],
                                     aws_secret_access_key=os.environ[s3_secret_key])
        self._s3 = self.session.resource(service_name='s3', endpoint_url=endpoint_url)
        self._bucket = self._s3.Bucket(bucket)

    def list_files_in_prefix(self, prefix: str):
        """
        Listing all files with a prefix on the S3 bucket

        :param prefix: prefix on the S3 bucket that should be filtered with

        returns:
            files: list of all files in the prefix
        """
        files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files

    def read_csv_to_df(self):
        """
        Read CSV file from S3 bucket and convert it to a Pandas DataFrame object
        """
        return True
    def write_df_to_s3(self):
        """
        Write the Pandas DataFrame to S3 bucket
        """
        return True
