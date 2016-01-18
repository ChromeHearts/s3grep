import io
import unittest

import s3grep


class TestS3Grep(unittest.TestCase):
    # aws public data set
    url = 's3://aws-publicdatasets/common-crawl/crawl-data/' \
          'CC-MAIN-2015-40/segment.paths.gz'

    def test_parse_url(self):
        bucket, myflie = s3grep._parse_url("s3://mybucket/myfile")

        self.assertEqual(bucket, "mybucket")
        self.assertEqual(myflie, "myfile")

    def test_grep_a_file(self):
        bucket, myfile = s3grep._parse_url(TestS3Grep.url)

        output = io.StringIO()
        s3grep._grep_a_file(bucketstr=bucket, key=myfile,
                            regex=r'.*1443737929054.*', output=output)

        self.assertEqual(output.getvalue(),
                         "common-crawl/crawl-data/CC-MAIN-2015-40/"
                         "segment.paths.gz:"
                         "common-crawl/crawl-data/CC-MAIN-2015-40/segments/"
                         "1443737929054.69/\n")
