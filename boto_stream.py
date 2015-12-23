import io

import botocore.response


class BotoStreamBody(io.RawIOBase):
    def __init__(self, body: botocore.response.StreamingBody,
                 content_length: int):
        self._body = body
        self._bufferleft = content_length

    def read(self, size=-1):
        if self._bufferleft <= 0:
            return None
        if size < 0:
            return self.readall()
        sizetoread = min(self._bufferleft, size)
        data = self._body.read(sizetoread)
        sizeread = len(data)
        self._bufferleft -= sizeread

        return data

    def readinto(self, b):
        if self._bufferleft <= 0:
            return 0
        sizetoread = len(b)
        data = self.read(sizetoread)
        if data is None:
            return 0
        sizeread = len(data)
        b[:sizeread] = data
        return sizeread

    def readall(self):
        return self.read()

    def readable(self):
        return self._bufferleft > 0
