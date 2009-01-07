
"""
fakehttp/socket implementation

- TrackerSocket: an object which masquerades as a socket and responds to
  requests in a manner consistent with a *very* stupid CloudFS tracker. 
   
- CustomHTTPConnection: an object which subclasses httplib.HTTPConnection
  in order to replace it's socket with a TrackerSocket instance.

The unittests each have setup methods which create freerange connection 
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

    def _create_GET_account_content(self, path, args):
        if args.has_key('format') and args['format'] == 'json':
            containers = []
            containers.append('[\n');
            containers.append('{"name":"container1","count":2,"bytes":78},\n')
            containers.append('{"name":"container2","count":1,"bytes":39},\n')
            containers.append('{"name":"container3","count":3,"bytes":117}\n')
            containers.append(']\n')
        elif args.has_key('format') and args['format'] == 'xml':
            containers = []
            containers.append('<?xml version="1.0" encoding="UTF-8"?>\n')
            containers.append('<account name="FakeAccount">\n')
            containers.append('<container><name>container1</name><count>2</count><bytes>78</bytes></container>\n')
            containers.append('<container><name>container2</name><count>1</count><bytes>39</bytes></container>\n')
            containers.append('<container><name>container3</name><count>3</count><bytes>117</bytes></container>\n')
            containers.append('</account>\n')
        else:
            containers = ['container%s\n' % i for i in range(1,4)]
        return ''.join(containers)

    def _create_GET_container_content(self, path, args):
        left = 0
        right = 9
        if args.has_key('offset'):
            left = int(args['offset'])
        if args.has_key('limit'):
            right = left + int(args['limit'])

        if args.has_key('format') and args['format'] == 'json':
            objects = []
            objects.append('{"name":"object1","hash":"4281c348eaf83e70ddce0e07221c3d28","bytes":14,"type":"application\/octet-stream"},\n')
            objects.append('{"name":"object2","hash":"b039efe731ad111bc1b0ef221c3849d0","bytes":64,"type":"application\/octet-stream"},\n')
            objects.append('{"name":"object3","hash":"4281c348eaf83e70ddce0e07221c3d28","bytes":14,"type":"application\/octet-stream"},\n')
            objects.append('{"name":"object4","hash":"b039efe731ad111bc1b0ef221c3849d0","bytes":64,"type":"application\/octet-stream"},\n')
            objects.append('{"name":"object5","hash":"4281c348eaf83e70ddce0e07221c3d28","bytes":14,"type":"application\/octet-stream"},\n')
            objects.append('{"name":"object6","hash":"b039efe731ad111bc1b0ef221c3849d0","bytes":64,"type":"application\/octet-stream"},\n')
            objects.append('{"name":"object7","hash":"4281c348eaf83e70ddce0e07221c3d28","bytes":14,"type":"application\/octet-stream"},\n')
            objects.append('{"name":"object8","hash":"b039efe731ad111bc1b0ef221c3849d0","bytes":64,"type":"application\/octet-stream"},\n')
            objects = objects[left:right]
            objects.insert(0, '[\n')
            objects.append(']\n')
        elif args.has_key('format') and args['format'] == 'xml':
            objects = []
            objects.append('<object><name>object1</name><hash>4281c348eaf83e70ddce0e07221c3d28</hash><bytes>14</bytes><type>application/octet-stream</type></object>\n')
            objects.append('<object><name>object2</name><hash>b039efe731ad111bc1b0ef221c3849d0</hash><bytes>64</bytes><type>application/octet-stream</type></object>\n')
            objects.append('<object><name>object3</name><hash>4281c348eaf83e70ddce0e07221c3d28</hash><bytes>14</bytes><type>application/octet-stream</type></object>\n')
            objects.append('<object><name>object4</name><hash>b039efe731ad111bc1b0ef221c3849d0</hash><bytes>64</bytes><type>application/octet-stream</type></object>\n')
            objects.append('<object><name>object5</name><hash>4281c348eaf83e70ddce0e07221c3d28</hash><bytes>14</bytes><type>application/octet-stream</type></object>\n')
            objects.append('<object><name>object6</name><hash>b039efe731ad111bc1b0ef221c3849d0</hash><bytes>64</bytes><type>application/octet-stream</type></object>\n')
            objects.append('<object><name>object7</name><hash>4281c348eaf83e70ddce0e07221c3d28</hash><bytes>14</bytes><type>application/octet-stream</type></object>\n')
            objects.append('<object><name>object8</name><hash>b039efe731ad111bc1b0ef221c3849d0</hash><bytes>64</bytes><type>application/octet-stream</type></object>\n')
            objects = objects[left:right]
            objects.insert(0, '<?xml version="1.0" encoding="UTF-8"?>\n')
            objects.insert(1, '<container name="test_container_1"\n')
            objects.append('</container>\n')
        else:
            objects = ['object%s\n' % i for i in range(1,9)]
            objects = objects[left:right]

        # prefix/order_by/path don't make much sense given our test data
        if args.has_key('prefix') or \
                args.has_key('order_by') or \
                args.has_key('path'):
            pass
        return ''.join(objects)

    def render_GET(self, path, args):
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
            content = self._create_GET_account_content(path, args)
        elif len(path) == 3:
            content = self._create_GET_container_content(path, args)
        # Object
        elif len(path) == 4:
            content = 'I am a teapot, short and stout\n'
        self.write('Content-Length: %d\n' % len(content))
        self.write('Connection: close\n\n')
        self.write(content)

    def render_HEAD(self, path, args):
        # Account
        if len(path) == 2:
            self.write('HTTP/1.1 204 No Content\n')
            self.write('Content-Type: text/plain\n')
            self.write('Connection: close\n')
            self.write('X-Account-Container-Count: 3\n')
            self.write('X-Account-Bytes-Used: 234\n\n')
        else:
            self.write('HTTP/1.1 200 Ok\n')
            self.write('Content-Type: text/plain\n')
            self.write('ETag: d5c7f3babf6c602a8da902fb301a9f27\n')
            self.write('Content-Length: 21\n')
            self.write('Connection: close\n\n')

    def render_POST(self, path, args):
        self.write('HTTP/1.1 202 Ok\n')
        self.write('Connection: close\n\n')

    def render_PUT(self, path, args):
        self.write('HTTP/1.1 200 Ok\n')
        self.write('Content-Type: text/plain\n')
        self.write('Connection: close\n\n')
    render_DELETE = render_PUT

    def render(self, method, uri):
        if '?' in uri:
            parts = uri.split('?')
            query = parts[1].strip('&').split('&')
            args = dict([tuple(i.split('=', 1)) for i in query])
            path = parts[0].strip('/').split('/')
        else:
            args = {}
            path = uri.strip('/').split('/')

        if hasattr(self, 'render_%s' % method):
            getattr(self, 'render_%s' % method)(path, args)
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
