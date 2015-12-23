import io
import unittest

import boto3

import boto_stream
import s3grep


class TestBotoStream(unittest.TestCase):
    def test_read(self):
        bucket, myfile = s3grep._parse_url(
                's3://eglp-core-temp/unit_test/s3grep_test.txt')
        resource = boto3.resource('s3')
        obj = resource.Object(bucket, myfile)
        datadict = obj.get()
        botostream = boto_stream.BotoStreamBody(
                body=datadict['Body'],
                content_length=obj.content_length)
        count = 1
        for line in io.TextIOWrapper(io.BufferedReader(botostream)):
            self.assertEquals('line {}\n'.format(count), line)
            count += 1
