import io

import botocore.response


class BotoStreamBody(io.RawIOBase):
    def __init__(self, body: botocore.response.StreamingBody):
        self._body = body

    def readinto(self, b):
        sizetoread = len(b)
        data = self._body.read(sizetoread)
        if data is None:
            return 0
        sizeread = len(data)
        b[:sizeread] = data
        return sizeread

    def readall(self):
        return self._body.read()

    def readable(self):
        return True
