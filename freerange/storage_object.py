
"""
Object operations

An Object is analogous to a file on a conventional filesystem. You can
read data from, or write data to your Objects. You can also associate 
arbitrary metadata with them.
"""

import md5, StringIO, mimetypes, os, tempfile
from urllib  import quote
from errors  import ResponseError, NoSuchObject, InvalidObjectName, \
                    InvalidObjectSize, IncompleteSend
from socket  import timeout
from consts  import user_agent
from utils   import requires_name

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

    @requires_name(InvalidObjectName)
    def read(self, size=-1, offset=0, hdrs=None):
        """
        Return the content of the remote storage object.
        
        Keyword arguments:
        size -- currently unimplemented
        offset -- currently unimplemented
        hdrs -- an optional dict of headers to send in the request
        
        Note: This method will buffer the entire response in memory. Use
        the stream() method if this isn't acceptable.
        """
        response = self.container.conn.make_request('GET', 
                path = [self.container.name, self.name], hdrs = hdrs)
        if (response.status < 200) or (response.status > 299):
            buff = response.read()
            raise ResponseError(response.status, response.reason)
        return response.read()
    
    @requires_name(InvalidObjectName)
    def stream(self, chunksize=8192, hdrs=None):
        """
        Return a generator of the remote storage object data.
        
        Keyword arguments:
        chunksize -- size in bytes yielded by the generator
        hdrs -- an optional dict of headers to send in the request
        
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
    
    @requires_name(InvalidObjectName)
    def sync_metadata(self):
        """
        Commits the metadata to the remote storage system.
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

    def __get_conn_for_write(self):
        headers = self._make_headers()

        headers['X-Storage-Token'] = self.container.conn.token

        path = "/%s/%s/%s" % (self.container.conn.uri.rstrip('/'), \
                quote(self.container.name), quote(self.name))

        # Requests are handled a little differently for writes ...
        http = self.container.conn._get_http_conn_instance()
        
        # TODO: more/better exception handling please
        http.putrequest('PUT', path)
        for hdr in headers:
            http.putheader(hdr, headers[hdr])
        http.putheader('User-Agent', user_agent)
        http.endheaders()
        return http
            
    # pylint: disable-msg=W0622
    @requires_name(InvalidObjectName)
    def write(self, data='', verify=True, callback=None):
        """
        Write data to the remote storage system.
        
        Keyword arguments:
        data -- the data to be written, a string or a file-like object
        verify -- enable/disable server-side checksum verification
        callback -- function to be used as a progress callback

        By default, server-side verification is enabled, (verify=True), and
        end-to-end verification is performed using an md5 checksum. When
        verification is disabled, (verify=False), the etag attribute will 
        be set to the value returned by the server, not one calculated 
        locally. When disabling verification, there is no guarantee that 
        what you think was uploaded matches what was actually stored. Use 
        this optional carefully. You have been warned.
        
        A callback can be passed in for reporting on the progress of
        the upload. The callback should accept two integers, the first
        will be for the amount of data written so far, the second for
        the total size of the transfer.
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

        http = self.__get_conn_for_write()
        
        response = None
        transfered = 0

        buff = data.read(4096)
        try:
            while len(buff) > 0:
                http.send(buff)
                buff = data.read(4096)
                transfered += len(buff)
                if callable(callback):
                    callback(transfered, self.size)
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

    @requires_name(InvalidObjectName)
    def send(self, iterable):
        """
        Write data to the remote storage system using a generator.
        
        Arguments:
        iterable -- a generator which yields the content to upload
        
        You must set the size attribute of the instance prior to calling
        this method. Failure to do so will result in an 
        InvalidObjectSize exception.
        
        If the generator raises StopIteration prior to yielding the 
        right number of bytes, an IncompleteSend exception is raised.
        
        If the content_type attribute is not set then a value of
        application/octet-stream will be used.
        
        Server-side verification will be performed if an md5 checksum is 
        assigned to the etag property before calling this method, 
        otherwise no verification will be performed, (verification
        can be performed afterward though by using the etag attribute
        which is set to the value returned by the server).
        """
        if not isinstance(self.size, (int, long)):
            raise InvalidObjectSize(self.size)
        
        # This method implicitly diables verification
        if not self._etag_override:
            self._etag = None
        
        if not self.content_type:
            self.content_type = 'application/octet-stream'
            
        http = self.__get_conn_for_write()
        
        response = None
        transferred = 0

        try:
            for chunk in iterable:
                http.send(chunk)
                transferred += len(chunk)
            # If the generator didn't yield enough data, stop, drop, and roll.
            if transferred < self.size:
                raise IncompleteSend()
            response = http.getresponse()
            buff = response.read()
        except timeout, err:
            if response:
                # pylint: disable-msg=E1101
                buff = response.read()
            raise err
        
        if (response.status < 200) or (response.status > 299):
            raise ResponseError(response.status, response.reason)

        for hdr in response.getheaders():
            if hdr[0].lower() == 'etag':
                self._etag = hdr[1]
            
    def load_from_filename(self, filename, verify=True, callback=None):
        """
        Put the contents of the named file into remote storage.
        """
        fobj = open(filename, 'rb')
        self.write(fobj, verify=verify, callback=callback)
        fobj.close()
        
    def _initialize(self):
        """
        Initialize the Object with values from the remote service, (if any).
        """
        if not self.name:
            return False
        
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
