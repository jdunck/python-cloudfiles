
"""
Object operations

An Object is analogous to a file on a conventional filesystem. You can
read data from, or write data to your Objects. You can also associate 
arbitrary metadata with them.
"""

import md5, StringIO, mimetypes, os, tempfile
from urllib  import quote
from errors  import ResponseError, NoSuchObject
from socket  import timeout
from consts  import user_agent

# Because HTTPResponse objects *have* to have read() called on them 
# before they can be used again ...
# pylint: disable-msg=W0612

class Object(object):
    """
    Storage data representing an Object, (metadata and data).
    """
    # R/O support of the legacy objsum attr.
    objsum = property(lambda self: self._etag)
    
    def __set_etag(self, value):
        self._etag = value
        self._etag_override = True
    
    etag = property(lambda self: self._etag, __set_etag)
    
    def __init__(self, container, name=None, force_exists=False):
        self.name = name
        self.container = container
        self.content_type = None
        self.size = None
        self.last_modified = None
        self._etag = None
        self._etag_override = False
        self.metadata = {}
        if not self._initialize() and force_exists:
            raise NoSuchObject(self.name)

    def read(self, size=-1, offset=0, hdrs=None):
        """
        Returns the Object data. The optional size and offset arguments are 
        reserved for a future enhancement and currently have no effect.
        """
        response = self.container.conn.make_request('GET', 
                path = [self.container.name, self.name], hdrs = hdrs)
        if (response.status < 200) or (response.status > 299):
            buff = response.read()
            raise ResponseError(response.status, response.reason)
        return response.read()
    
    def stream(self, chunksize=8192, hdrs=None):
        """
        Returns a generator that can be used to iterate the Object data in 
        chunks of size "chunksize", (defaults to 8K bytes).
        
        Warning: The HTTP response is only complete after this generator
        has raised a StopIteration. No other methods can be called until
        this has occurred.
        """
        response = self.container.conn.make_request('GET', 
                path = [self.container.name, self.name], hdrs = hdrs)
        if response.status < 200 or response.status > 299:
            buff = response.read()
            raise ResponseError(response.status, response.reason)
        buff = response.read(chunksize)
        while len(buff) > 0:
            yield buff
            buff = response.read(chunksize)
        # I hate you httplib
        buff = response.read()
            
    def sync_metadata(self):
        """
        Writes all metadata for the Object.
        """
        if self.metadata:
            headers = self._make_headers()
            headers['Content-Length'] = 0
            response = self.container.conn.make_request(
                'POST', [self.container.name, self.name], hdrs=headers, data=''
            )
            buff = response.read()
            if response.status != 202:
                raise ResponseError(response.status, response.reason)

    # pylint: disable-msg=W0622
    def write(self, data='', verify=True):
        """
        Write data to remote storage. Accepts either a string containing
        the data to be written, or an open file object.
        
        Server-side checksum verification can be disabled by passing a
        value that will evaluate as False using the verify keyword 
        argument. When the write is complete, the etag attribute will 
        be populated with the value returned from the server, NOT one
        calculated locally. Warning: When disabling verification, 
        there is no guarantee that what you think was uploaded matches
        what was actually stored. Use this optional carefully. You have
        been warned.
        """
        if isinstance(data, file):
            # pylint: disable-msg=E1101
            try:
                data.flush()
            except IOError:
                pass # If the file descriptor is read-only this will fail
            self.size = int(os.fstat(data.fileno())[6])
        else:
            data = StringIO.StringIO(data)
            self.size = data.len
            
        # If override is set (and _etag is not None), then the etag has
        # been manually assigned and we will not calculate our own.
        if verify:
            if not self._etag_override:
                self._etag = Object.compute_md5sum(data)
        else:
            self._etag = None
            
        if not self.content_type:
            # pylint: disable-msg=E1101
            type = None
            if hasattr(data, 'name'):
                type = mimetypes.guess_type(data.name)[0]
            self.content_type = type and type or 'application/octet-stream'
        headers = self._make_headers()

        headers['X-Storage-Token'] = self.container.conn.token

        path = "/%s/%s/%s" % (self.container.conn.uri.rstrip('/'), \
                quote(self.container.name), quote(self.name))

        # Requests are handled a little differently here ...
        http = self.container.conn._get_http_conn_instance()

        # TODO: more/better exception handling please --------------------
        http.putrequest('PUT', path)
        for hdr in headers:
            http.putheader(hdr, headers[hdr])
        http.putheader('User-Agent', user_agent)
        http.endheaders()

        response = None

        buff = data.read(4096)
        try:
            while len(buff) > 0:
                http.send(buff)
                buff = data.read(4096)
            response = http.getresponse()
            buff = response.read()
        except timeout, err:
            if response:
                # pylint: disable-msg=E1101
                buff = response.read()
            raise err
        # ----------------------------------------------------------------

        if (response.status < 200) or (response.status > 299):
            raise ResponseError(response.status, response.reason)

        # If verification has been disabled for this write, then set the 
        # instances etag attribute to what the server returns to us.
        if not verify:
            for hdr in response.getheaders():
                if hdr[0].lower() == 'etag':
                    self._etag = hdr[1]

    def load_from_filename(self, filename, verify=True):
        """
        Put the contents of the named file into remote storage.
        """
        fobj = open(filename, 'rb')
        self.write(fobj, verify=verify)
        fobj.close()
        
    def _initialize(self):
        """
        Initialize the Object with values from the remote service, (if any).
        """
        response = self.container.conn.make_request(
                'HEAD', [self.container.name, self.name]
        )
        buff = response.read()
        if response.status == 404: 
            return False
        if (response.status < 200) or (response.status > 299):
            raise ResponseError(response.status, response.reason)
        for hdr in response.getheaders():
            if hdr[0].lower() == 'content-type':
                self.content_type = hdr[1]
            if hdr[0].lower().startswith('x-object-meta-'):
                self.metadata[hdr[0][14:]] = hdr[1]
            if hdr[0].lower() == 'etag':
                self._etag = hdr[1]
                self._etag_override = False
            if hdr[0].lower() == 'content-length':
                self.size = int(hdr[1])
            if hdr[0].lower() == 'last-modified':
                self.last_modified = hdr[1]
        return True

    def __str__(self):
        return self.name

    def _make_headers(self):
        """
        Returns a dictionary representing http headers based on the 
        respective instance attributes.
        """
        headers = {}
        headers['Content-Length'] = self.size and self.size or 0
        if self._etag: headers['ETag'] = self._etag

        if self.content_type: headers['Content-Type'] = self.content_type
        else: headers['Content-Type'] = 'application/octet-stream'

        for key in self.metadata:
            headers['X-Object-Meta-'+key] = self.metadata[key]
        return headers

    def compute_md5sum(cls, fobj):
        """
        Given an open file object, returns the md5 hexdigest of the data.
        """
        checksum = md5.new()
        buff = fobj.read(4096)
        while buff:
            checksum.update(buff)
            buff = fobj.read(4096)
        fobj.seek(0)
        return checksum.hexdigest()
    compute_md5sum = classmethod(compute_md5sum)

class ObjectResults(object):
    """
    An iterable results set object for Objects.
    """
    def __init__(self, container, objects=None):
        self._objects = objects and objects or list()
        self.container = container

    def __getitem__(self, key):
        return Object(self.container, self._objects[key])

    def __getslice__(self, i, j):
        return [Object(self.container, k) for k in self._objects[i:j]]

    def __contains__(self, item):
        return item in self._objects

    def __len__(self):
        return len(self._objects)

    def __repr__(self):
        return repr(self._objects)

    def index(self, value, *args):
        """
        returns an integer for the first index of value
        """
        return self._objects.index(value, *args)

    def count(self, value):
        """
        returns the number of occurrences of value
        """
        return self._objects.count(value)

# vim:set ai sw=4 ts=4 tw=0 expandtab:
