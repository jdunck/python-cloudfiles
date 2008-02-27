
"""
egg operations

An Egg is analogous to a file on a conventional filesystem. You can
read data from, or write data to your eggs. You can also associate 
arbitrary metadata with them.
"""

import md5, StringIO, mimetypes, os, tempfile
from urllib  import quote
from errors  import ResponseError, NoSuchEgg
from socket  import timeout
from consts  import user_agent

# Because HTTPResponse objects *have* to have read() called on them 
# before they can be used again ...
# pylint: disable-msg=W0612

class Egg(object):
    """
    Object representing an Egg, (metadata and data).
    """
    # R/O support of the legacy eggsum attr.
    eggsum = property(lambda self: self.etag)
    
    def __init__(self, basket, name=None, force_exists=False):
        self.name = name
        self.basket = basket
        self.content_type = None
        self.size = None
        self.etag = None
        self.metadata = {}
        if not self._initialize() and force_exists:
            raise NoSuchEgg(self.name)

    def read(self, size=-1, offset=0):
        """
        Returns the egg data. The optional size and offset arguments are 
        reserved for a future enhancement and currently have no effect.
        """
        response = self.basket.conn.make_request('GET', 
                [self.basket.name, self.name])
        if (response.status < 200) or (response.status > 299):
            raise ResponseError(response.status, response.reason)
        return response.read()
    
    def stream(self, size=-1, offset=0, chunksize=8192):
        """
        Returns a generator that can be used to iterate the egg data in 
        chunks of size "chunksize", (defaults to 8K bytes).
        
        Warning: The HTTP response is only complete after this generator
        has raised a StopIteration. No other methods can be called until
        this has occurred.
        """
        response = self.basket.conn.make_request('GET', 
                [self.basket.name, self.name])
        if response.status < 200 or response.status > 299:
            raise ResponseError(response.status, response.reason)
        buff = response.read(chunksize)
        while len(buff) > 0:
            yield buff
            buff = response.read(chunksize)
        # I hate you httplib
        buff = response.read()
            
    def sync_metadata(self):
        """
        Writes all metadata for the egg.
        """
        if self.metadata:
            headers = self._make_headers()
            headers['Content-Length'] = 0
            response = self.basket.conn.make_request(
                'POST', [self.basket.name, self.name], hdrs=headers, data=''
            )
            buff = response.read()
            if response.status != 202:
                raise ResponseError(response.status, response.reason)

    # pylint: disable-msg=W0622
    def write(self, data=''):
        """
        Write data to remote storage. Accepts either a string containing
        the data to be written, or an open file object.
        """
        if isinstance(data, file):
            # pylint: disable-msg=E1101
            data.flush()
            self.size = int(os.fstat(data.fileno())[6])
        else:
            data = StringIO.StringIO(data)
            self.size = data.len
            
        # Headers
        self.etag = Egg.compute_md5sum(data)
        if not self.content_type:
            # pylint: disable-msg=E1101
            type = None
            if hasattr(data, 'name'):
                type = mimetypes.guess_type(data.name)[0]
            self.content_type = type and type or 'application/octet-stream'
        headers = self._make_headers()

        headers['X-Storage-Token'] = self.basket.conn.token

        path = "/%s/%s/%s" % (self.basket.conn.uri.rstrip('/'), \
                quote(self.basket.name), quote(self.name))

        # Requests are handled a little differently here ...
        http = self.basket.conn._get_http_conn_instance()

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

    def load_from_filename(self, filename):
        """
        Put the contents of the named file into remote storage.
        """
        fobj = open(filename, 'rb+')
        self.write(fobj)
        fobj.close()
        
    def _initialize(self):
        """
        Initialize the Egg with values from the remote service, (if any).
        """
        response = self.basket.conn.make_request(
                'HEAD', [self.basket.name, self.name]
        )
        buff = response.read()
        if response.status == 404: 
            return False
        if (response.status < 200) or (response.status > 299):
            raise ResponseError(response.status, response.reason)
        for hdr in response.getheaders():
            if hdr[0].lower() == 'content-type':
                self.content_type = hdr[1]
            if hdr[0].lower().startswith('x-egg-meta-'):
                self.metadata[hdr[0][11:]] = hdr[1]
            if hdr[0].lower() == 'etag':
                self.etag = hdr[1]
            if hdr[0].lower() == 'content-length':
                self.size = int(hdr[1])
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
        if self.etag: headers['ETag'] = self.etag

        if self.content_type: headers['Content-Type'] = self.content_type
        else: headers['Content-Type'] = 'application/octet-stream'

        for key in self.metadata:
            headers['X-Egg-Meta-'+key] = self.metadata[key]
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

class EggResults(object):
    """
    An iterable results set object for Eggs.
    """
    def __init__(self, basket, eggs=list()):
        self._eggs = eggs
        self.basket = basket

    def __getitem__(self, key):
        return Egg(self.basket, self._eggs[key])

    def __getslice__(self, i, j):
        return [Egg(self.basket, k) for k in self._eggs[i:j]]

    def __contains__(self, item):
        return item in self._eggs

    def __len__(self):
        return len(self._eggs)

    def __repr__(self):
        return repr(self._eggs)

    def index(self, value, *args):
        """
        returns an integer for the first index of value
        """
        return self._eggs.index(value, *args)

    def count(self, value):
        """
        returns the number of occurrences of value
        """
        return self._eggs.count(value)

# vim:set ai sw=4 ts=4 tw=0 expandtab:
