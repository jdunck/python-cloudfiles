
"""
authentication operations

Authentication instances are used to interact with the remote 
authentication service, retreiving storage system routing information
and session tokens.
"""

import urllib
from httplib  import HTTPSConnection, HTTPConnection, HTTPException
from utils    import parse_url
from errors   import ResponseError
from consts   import user_agent, default_api_version

class BaseAuthentication(object):
    """
    The base authentication class from which all others inherit.
    """
    def __init__(self, account, username, password, url):
        self.account = account
        self.url = url
        self.headers = dict()
        self.headers['X-Storage-User'] = username
        self.headers['X-Storage-Pass'] = password
        self.headers['User-Agent'] = user_agent
        (self.host, self.port, self.uri, self.is_ssl) = parse_url(url)
        self.conn_class = self.is_ssl and HTTPSConnection or HTTPConnection
        
    def _get_uri(self, version):
        auth_uri = "/v%d/%s/auth" % (int(version), self.account)
        
        # Prefix the uri with any fragment parsed from the original url.
        if len(self.uri):
            auth_uri = "/%s%s" % (self.uri, auth_uri)
            
        return urllib.quote(auth_uri)
        
    def authenticate(self, version=default_api_version):
        """
        Initiates authentication with the remote service and returns a 
        two-tuple containing the storage system URL and session token.
        Accepts a single (optional) argument for the storage system
        API version.
        
        Note: This is a dummy method from the base class. It must be
        overridden by sub-classes.
        """
        return (None, None)

class MockAuthentication(BaseAuthentication):
    """
    A dummy authentication class used for testing purposes. Not to be
    used in a production setting.
    
    """
    def authenticate(self, version):
        """
        Given an api version, returns the URL passed during instantiation
        with API version and account information appended, and a bogus
        session token.
        """
        storage_url = "%s/v%d/%s" % \
                (self.url.rstrip('/'), int(version), self.account)
        storage_token = "xxxxxxxxxxxxxxxxxxxxxx"
        return (storage_url, storage_token)

class Authentication(BaseAuthentication):
    """
    Authentication, routing, and session token management.
    """
    def authenticate(self, version=default_api_version):
        """
        Initiates authentication with the remote service and returns a 
        two-tuple containing the storage system URL and session token.
        Accepts a single (optional) argument for the storage system
        API version.
        """
        conn = self.conn_class(self.host, self.port)
        conn.request('GET', self._get_uri(version), '', self.headers)
        response = conn.getresponse()
        buff = response.read()

        # A status code of 401 indicates that the supplied credentials
        # were not accepted by the authentication service.
        if response.status == 401:
            raise AuthenticationFailed()
        
        if response.status != 204:
            raise ResponseError(response.status, response.reason)

        storage_url = storage_token = None
        
        for hdr in response.getheaders():
            if hdr[0].lower() == "x-storage-url":
                storage_url = hdr[1]
            if hdr[0].lower() == "x-storage-token":
                storage_token = hdr[1]

        conn.close()

        if not (storage_token and storage_url):
            raise AuthenticationError("Invalid response from the " \
                    "authentication service.")
        
        return (storage_url, storage_token)
    
class AuthenticationFailed(Exception):
    """
    Raised on a failure to authenticate.
    """
    pass

class AuthenticationError(Exception):
    """
    Raised when an unspecified authentication error has occurred.
    """
    pass
        
# vim:set ai ts=4 sw=4 tw=0 expandtab:
