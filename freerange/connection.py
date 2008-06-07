
"""
connection operations

Connection instances are used to communicate with the remote service at
the account level creating, listing and deleting Containers, and returning
Container instances.
"""

import  socket
from    urllib    import quote
from    httplib   import HTTPSConnection, HTTPConnection, HTTPException
from    container import Container, ContainerResults
from    utils     import parse_url
from    errors    import ResponseError, NoSuchContainer, ContainerNotEmpty
from    Queue     import Queue, Empty, Full
from    time      import time
from    consts    import __version__, user_agent, default_api_version
from    authentication import Authentication

# TODO: there should probably be a way of getting at the account and 
# url from the Connection class without having to pass it during 
# instantiation.

# Because HTTPResponse objects *have* to have read() called on them 
# before they can be used again ...
# pylint: disable-msg=W0612

class Connection(object):
    """
    Manages the connection to the storage system and serves as a factory 
    for Container instances.
    """
    def __init__(self, account=None, username=None, password=None, \
            authurl=None, **kwargs):
        """
        Accepts keyword arguments for account, username, password and
        authurl. Optionally, you can omit these keywords and supply an
        Authentication object using the auth keyword.
        
        It is also possible to set the storage system API version and 
        timeout values using the api_version and timeout keyword 
        arguments respectively.
        """
        self.debuglevel = int(kwargs.get('debuglevel', 0))
        socket.setdefaulttimeout = int(kwargs.get('timeout', 5))
        self.api_version = kwargs.get('api_version', default_api_version)
        self.auth = kwargs.has_key('auth') and kwargs['auth'] or None
        
        if not self.auth:
            if (account and username and password and authurl):
                self.auth = Authentication(account, username, password, authurl)
            else:
                raise TypeError("Incorrect or invalid arguments supplied")
        
        self.connection = None
        self._authenticate()
        
    def _authenticate(self):
        """
        Authenticate and setup this instance with the values returned.
        """
        (url, self.token) = self.auth.authenticate(self.api_version)
        (self.host, self.port, self.uri, self.is_ssl) = parse_url(url)
        self.conn_class = self.is_ssl and HTTPSConnection or HTTPConnection
        self.http_connect()

    def http_connect(self):
        """
        Setup the http connection instance.
        """
        self.connection = self._get_http_conn_instance()
        self.connection.set_debuglevel(self.debuglevel)
        
    def _get_http_conn_instance(self):
        return self.conn_class(self.host, port=self.port)

    def make_request(self, method, path=[], data='', hdrs={}, parms={}):
        """
        Given a method (i.e. GET, PUT, POST, etc), a path, data, header and
        metadata dicts, and an option dictionary of query parameters, 
        performs an http request.
        """
        path = '/%s/%s' % \
                 (self.uri.rstrip('/'), '/'.join([quote(i) for i in path]))
        
        if isinstance(parms, dict) and parms:
            query_args = \
                ['%s=%s' % (quote(x),quote(str(y))) for (x,y) in parms.items()]
            path = '%s?%s' % (path, '&'.join(query_args))
            
        headers = {'Content-Length': len(data), 'User-Agent': user_agent, 
               'X-Storage-Token': self.token}
        isinstance(hdrs, dict) and headers.update(hdrs)
        
        # Send the request
        self.connection.request(method, path, data, headers)

        def retry_request():
            self.http_connect()
            self.connection.request(method, path, data, headers)
            return self.connection.getresponse()

        try:
            response = self.connection.getresponse()
# in case you ever really, really need to know
#         except httplib.NotConnected:
#             print "NotConnected"
#             response = retry_request()
#         except httplib.InvalidURL:
#             print "InvalidURL"
#             response = retry_request()
#         except httplib.UnknownProtocol:
#             print "UnknownProtocol"
#             response = retry_request()
#         except httplib.UnknownTransferEncoding:
#             print "UnknownTransferEncoding"
#             response = retry_request()
#         except httplib.UnimplementedFileMode:
#             print "UnimplementedFileMode"
#             response = retry_request()
#         except httplib.IncompleteRead:
#             print "IncompleteRead"
#             response = retry_request()
#         except httplib.CannotSendRequest:
#             print "CannotSendRequest"
#             response = retry_request()
#         except httplib.CannotSendHeader:
#             print "CannotSendHeader"
#             response = retry_request()
#         except httplib.ResponseNotReady:
#             print "ResponseNotReady"
#             response = retry_request()
#         except httplib.BadStatusLine:
#             print "BadStatusLine"
#             response = retry_request()
#         except httplib.ImproperConnectionState:
#             print "ImproperConnectionState"
#             response = retry_request()
        except HTTPException:
#            print "HTTPException"
            response = retry_request()
            
        if response.status == 401:
            self._authenticate()
            response = retry_request()

        return response

    def create_container(self, container_name):
        """
        Given a Container name, returns a Container item, creating a new
        Container if one does not already exist.
        """
        response = self.make_request('PUT', [container_name])
        buff = response.read()
        if (response.status < 200) or (response.status > 299):
            raise ResponseError(response.status, response.reason)
        return Container(self, container_name)

    def delete_container(self, container_name):
        """
        Given a Container name, delete it.
        """
        if isinstance(container_name, Container):
            container_name = container_name.name
        response = self.make_request('DELETE', [container_name])
        buff = response.read()
        
        if (response.status == 409):
            raise ContainerNotEmpty(container_name)
        elif (response.status < 200) or (response.status > 299):
            raise ResponseError(response.status, response.reason)

    def get_all_containers(self):
        """
        Returns a Container item result set.
        """
        return ContainerResults(self, self.list_containers())

    def get_container(self, container_name):
        """
        Return a single Container item for the given Container.
        """
        response = self.make_request('HEAD', [container_name])
        count = size = None
        for hdr in response.getheaders():
            if hdr[0].lower() == 'x-container-object-count':
                try: count = int(hdr[1])
                except: count = 0
            if hdr[0].lower() == 'x-container-bytes-used':
                try: size = int(hdr[1])
                except: size = 0
        buff = response.read()
        if response.status == 404:
            raise NoSuchContainer(container_name)
        if (response.status < 200) or (response.status > 299):
            raise ResponseError(response.status, response.reason)
        return Container(self, container_name, count, size)

    def list_containers(self):
        """
        Returns a list of Containers.
        """
        response = self.make_request('GET', [''])
        if (response.status < 200) or (response.status > 299):
            buff = response.read()
            raise ResponseError(response.status, response.reason)
        return response.read().splitlines()

    def __getitem__(self, key):
        return self.get_container(key)


class ConnectionPool(Queue):
    """
    A thread-safe connection pool object.
    """
    def __init__(self, account=None, username=None, password=None, \
            authurl=None, **kwargs):
        auth = kwargs.get('auth', None)
        self.timeout = kwargs.get('timeout', 5)
        self.connargs = dict(account=account, username=username, 
            password=password, authurl=authurl, auth=auth, timeout=self.timeout)
        poolsize = kwargs.get('poolsize', 10)
        Queue.__init__(self, poolsize)

    def get(self):
        """
        Return a freerange connection object.
        """
        try:
            (create, connobj) = Queue.get(self, block=0)
        except Empty:
            connobj = Connection(**self.connargs)
        return connobj

    def put(self, connobj):
        """
        Place a freerange connection object back into the pool.
        """
        try:
            Queue.put(self, (time(), connobj), block=0)
        except Full:
            del connobj


if __name__ == '__main__':
    # pylint: disable-msg=C0103
    from authentication import MockAuthentication
    auth = MockAuthentication('eevans', 'eevans', 'bogus', 'http://10.0.0.4/')
    conn = Connection(auth=auth)
    containers = conn.get_all_containers()
    print auth.account
    for container in containers:
        print " \_", container.name
        print "   \_", ', '.join(container.list_objects())

# vim:set ai sw=4 ts=4 tw=0 expandtab:
