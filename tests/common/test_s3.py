"""TestS3BucketConnectorMethods"""
import sys
import os
import unittest
import boto3
from moto import mock_aws

# Append the src directory to PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.common.s3 import S3BucketConnector # pylint: disable

class TestS3BucketConnectorMethods(unittest.TestCase):
    """
    Testing the S3BucketConnector class
    """
    def setUp(self):
        """
        Setting up the environment
        """
        # mocking s3 connection start
        self.mock_s3 = mock_aws()
        self.mock_s3.start()
        # defining the class arguments
        self.s3_access_key = 'AWS_ACCESS_KEY_ID'
        self.s3_secret_key = 'AWS_ACCESS_SECRET_KEY'
        self.s3_endpoint_url = 'https://s3.ap-southeast-2.amazonaws.com'
        self.s3_bucket_name = 'test_bucket'
        # Create s3 access key as environment variables
        os.environ[self.s3_access_key] = 'KEY1'
        os.environ[self.s3_secret_key] = 'KEY2'
        # Create a bucket on the mocked s3
        self.s3 = boto3.resource(service_name='s3', endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name,
                              CreateBucketConfiguration={
                                 'LocationConstraint': 'ap-southeast-2'
                              })
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)
        # Create a testing instance
        self.s3_bucket_conn = S3BucketConnector(self.s3_access_key,
                                                self.s3_secret_key,
                                                self.s3_endpoint_url,
                                                self.s3_bucket_name)
    def tearDown(self):
        """
        Executing after unittests
        """
        self.mock_s3.stop()

    def test_list_files_in_prefix_correct(self):
        """
        Testing list_files_in_prefix method for getting 2 file keys
        as list on the mocked s3 bucket
        """
        # Expected results
        prefix_expect = 'prefix/'
        key1_expect = f'{prefix_expect}test1.csv'
        key2_expect = f'{prefix_expect}test2.csv'
        # Test init
        csv_content = """col1,col2
        valA,valB"""
        self.s3_bucket.put_object(Body=csv_content, Key=key1_expect)
        self.s3_bucket.put_object(Body=csv_content, Key=key2_expect)
        # Method execution
        list_result = self.s3_bucket_conn.list_files_in_prefix(prefix_expect)
        # Tests after method excution
        self.assertEqual(len(list_result), 2)
        self.assertIn(key1_expect, list_result)
        self.assertIn(key2_expect, list_result)
        # Clean up after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects':[
                    {
                        'Key': key1_expect
                    },
                    {
                        'Key': key2_expect
                    }
                ]
            }
        )

    def test_list_files_in_prefix_wrong(self):
        """
        Testing list_files_in_prefix method in case of a wrong
        or not existing prefix
        """
        return True

if __name__=="__main__":
    unittest.main()
    # testInstance = TestS3BucketConnectorMethods()
    # testInstance.setUp()
    # testInstance.test_list_files_in_prefix_correct()
    # testInstance.tearDown()
