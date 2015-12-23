import gzip
import io
import tempfile
import unittest

import boto3

import boto_stream
import s3grep


class TestBotoStream(unittest.TestCase):
    # aws public data set
    url = 's3://aws-publicdatasets/common-crawl/crawl-data/' \
          'CC-MAIN-2015-40/segment.paths.gz'

    def test_read_full_binary_file(self):
        bucket, myfile = s3grep._parse_url(TestBotoStream.url)
        resource = boto3.resource('s3')
        obj = resource.Object(bucket, myfile)

        datadict = obj.get()
        botostream = boto_stream.BotoStreamBody(body=datadict['Body'])

        reader = io.BufferedReader(botostream)

        # use the regular boto3 api
        with tempfile.NamedTemporaryFile('wb') as tfile:
            obj2 = resource.Object(bucket, myfile)
            obj2.download_file(tfile.name)

            with open(tfile.name, 'rb') as rtfile:
                self.assertEqual(rtfile.read(), reader.read())

    def test_read_gunzip_file(self):
        bucket, myfile = s3grep._parse_url(TestBotoStream.url)
        resource = boto3.resource('s3')
        obj3 = resource.Object(bucket, myfile)
        datadict = obj3.get()
        buffr = io.BufferedReader(boto_stream.BotoStreamBody(datadict['Body']))
        reader = io.TextIOWrapper(gzip.GzipFile(fileobj=buffr, mode='rb'))

        # check the first line
        self.assertEqual(next(reader),
                         "common-crawl/crawl-data/CC-MAIN-2015-40/segments/"
                         "1443736672328.14/\n")
