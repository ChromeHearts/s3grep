import argparse
import gzip
import io
import logging
import os
import re
import sys
import urllib.parse

import boto3

logger = logging.getLogger('s3grep')


def _parse_url(url: str) -> (str, str):
    '''
    get the bucket and path
    :param url: full s3 path
    :return:  bucket, path
    '''
    splitresult = urllib.parse.urlsplit(url, allow_fragments=False)

    #  remove the first / from the path
    return splitresult.netloc, splitresult.path[1:]


def _parse_args(argv):
    parser = argparse.ArgumentParser(
            prog=argv[0], description='s3grep to grep some pattern'
                                      ' from files on s3')

    parser.add_argument('-u', '--url',
                        dest='url',
                        type=str,
                        help='the S3 location(s) of file(s)',
                        required=True)
    parser.add_argument('-d', '--debug',
                        dest='debug',
                        type=bool,
                        help='turn on the debug mode with high verbosity',
                        action='store_true',
                        default=False)
    parser.add_argument('-r', '--regex',
                        dest='regex',
                        type=str,
                        help='the regex pattern used for line matching',
                        required=True)

    return parser.parse_args(argv[1:])


def _setup_logging(verbose: bool):
    logging.basicConfig(
            format='%(asctime)s-%(levelname)s-%(name)s:%(lineno)d: %(message)')

    l = logging.getLogger()

    if verbose is True:
        l.setLevel(logging.DEBUG)
    else:
        l.setLevel(logging.INFO)


def _grep_a_file(bucket: str, key: str, regex: str, output: io.TextIOBase):
    '''
    parse the s3 file line to see if it matches the regex
    if yes, dump the line into output buffer

    :param bucket:
    :param key:
    :param regex:
    :param output: the output buffer
    :return:
    '''
    s3 = boto3.resource('s3')
    object = s3.Object(bucket, key)

    datadict = object.get()

    instream = datadict['Body']

    filename, file_extension = os.path.splitext(key)

    if file_extension == '.gz':
        instream = gzip.GzipFile(fileobj=instream, mode='rb')

    for line in io.TextIOWrapper(instream):
        if re.match(regex, line) is not None:
            output.write(line)


def main():
    args = _parse_args(sys.argv)
    _setup_logging(args.debug)
    bucket, key = _parse_url(args.url)

    _grep_a_file(bucket=bucket, key=key, regex=args.regex, output=sys.stdout)

    pass


if __name__ == '__main__':
    main()
