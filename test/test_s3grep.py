import io
import unittest

import s3grep


class TestS3Grep(unittest.TestCase):
    def test_parse_url(self):
        bucket, myflie = s3grep._parse_url("s3://mybucket/somepath/myfile")

        self.assertEqual(bucket, "mybucket")
        self.assertEqual(myflie, "somepath/myfile")

    def test_grep_a_file(self):
        bucket, myfile = s3grep._parse_url(
            's3://eglp-core-temp/unit_test/s3grep_test.txt')

        #buf = io.StringIO()

        #s3grep._grep_a_file(bucket=bucket, key=myfile, regex=r'1', output=buf)

        #buf.seek(0)
        #self.assertEqual('line 1', buf.read())
