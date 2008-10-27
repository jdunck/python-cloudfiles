
"""
fakehttp/socket implementation

- TrackerSocket: an object which masquerades as a socket and responds to
  requests in a manner consistent with a *very* stupid cloudfiles tracker. 
   
- CustomHTTPConnection: an object which subclasses httplib.HTTPConnection
  in order to replace it's socket with a TrackerSocket instance.

The unittests each have setup methods which create cloudfiles connection 
instances that have had their HTTPConnection instances replaced by 
intances of CustomHTTPConnection.
"""

from httplib import HTTPConnection as connbase
import StringIO

class FakeSocket(object):
    def __init__(self):
        self._rbuffer = StringIO.StringIO()
        self._wbuffer = StringIO.StringIO()

    def close(self):
        pass

    def send(self, data, flags=0):
        self._rbuffer.write(data)
    sendall = send

    def recv(self, len=1024, flags=0):
        return self._wbuffer(len)

    def connect(self):
        pass

    def makefile(self, mode, flags):
        return self._wbuffer

class TrackerSocket(FakeSocket):
    def write(self, data):
        self._wbuffer.write(data)
    def read(self, length=-1):
        return self._rbuffer.read(length)

    def render_GET(self, path):
        # Special path that returns 404 Not Found
        if (len(path) == 4) and (path[3] == 'bogus'):
            self.write('HTTP/1.1 404 Not Found\n')
            self.write('Content-Type: text/plain\n')
            self.write('Content-Length: 0\n')
            self.write('Connection: close\n\n')
            return
        self.write('HTTP/1.1 200 Ok\n')
        self.write('Content-Type: text/plain\n')
        if len(path) == 2:
            self.write('Content-Length: 33\n')
            self.write('Connection: close\n\n')
            self.write('container1\n')
            self.write('container2\n')
            self.write('container3\n')
        if len(path) == 3:
            objects = ['object%s\n' % i for i in range(1,9)]
            resrc = path[2]
            # Support for the optional limit query parm
            if '?' in resrc:
                query = resrc[resrc.find('?')+1:].strip('&').split('&')
                args = dict([tuple(i.split('=', 1)) for i in query])
                if args.has_key('limit'):
                    objects = objects[:int(args['limit'])]
            content = ''.join(objects)
            self.write('Content-Length: %d\n' % len(content))
            self.write('Connection: close\n\n')
            self.write(content)
        if len(path) == 4:
            self.write('Content-Length: 31\n')
            self.write('Connection: close\n\n')
            self.write('I am a teapot, short and stout\n')

    def render_HEAD(self, path):
        if len(path) == 2:
            self.write('HTTP/1.1 204 Ok\n')
            self.write('Content-Type: text/plain\n')
            self.write('X-Account-Container-Count: 1\n')
            self.write('X-Account-Bytes-Used: 79\n')
            self.write('Connection: close\n\n')
        else:
            self.write('HTTP/1.1 200 Ok\n')
            self.write('Content-Type: text/plain\n')
            self.write('ETag: d5c7f3babf6c602a8da902fb301a9f27\n')
            self.write('Content-Length: 21\n')
            self.write('Connection: close\n\n')

    def render_POST(self, path):
        self.write('HTTP/1.1 202 Ok\n')
        self.write('Connection: close\n\n')

    def render_PUT(self, path):
        self.write('HTTP/1.1 200 Ok\n')
        self.write('Content-Type: text/plain\n')
        self.write('Connection: close\n\n')
    render_DELETE = render_PUT

    def render(self, method, uri):
        path = uri.strip('/').split('/')
        if hasattr(self, 'render_%s' % method):
            getattr(self, 'render_%s' % method)(path)
        else:
            self.write('HTTP/1.1 406 Not Acceptable\n')
            self.write('Content-Type: text/plain\n')
            self.write('Connection: close\n')

    def makefile(self, mode, flags):
        self._rbuffer.seek(0)
        lines = self.read().splitlines()
        (method, uri, version) = lines[0].split()

        self.render(method, uri)

        self._wbuffer.seek(0)
        return self._wbuffer

class CustomHTTPConnection(connbase):
    def connect(self):
        self.sock = TrackerSocket()


if __name__ == '__main__':
    conn = CustomHTTPConnection('localhost', 8000)
    conn.request('HEAD', '/v1/account/container/object')
    response = conn.getresponse()
    print "Status:", response.status, response.reason
    for (key, value) in response.getheaders():
        print "%s: %s" % (key, value)
    print response.read()


# vim:set ai sw=4 ts=4 tw=0 expandtab:
