
import unittest
from freerange.authentication import BaseAuthentication as Auth
from misc import printdoc

class AuthenticationTest(unittest.TestCase):
    """
    Freerange Authentication class tests.
    """
    @printdoc
    def test_get_uri(self):
        """
        Validate authentication uri construction.
        """
        assert self.auth._get_uri(1) == "/v1/fakeaccount/auth", \
               "authentication URL was not properly constructed"
        
    @printdoc
    def test_authenticate(self):
        """
        Sanity check authentication method stub (lame).
        """
        assert self.auth.authenticate() == (None, None), \
               "authenticate() did not return a two-tuple"
        
    @printdoc
    def test_headers(self):
        """
        Ensure headers are being set.
        """
        assert self.auth.headers['X-Storage-User'] == 'jsmith', \
               "storage user header not properly assigned"
        assert self.auth.headers['X-Storage-Pass'] == 'qwerty', \
               "storage password header not properly assigned"
        
    def setUp(self):
        self.auth = Auth('fakeaccount', 'jsmith', 'qwerty', 'http://localhost')
    def tearDown(self):
        del self.auth

# vim:set ai ts=4 tw=0 sw=4 expandtab: