
import unittest
from cloudfiles.authentication import BaseAuthentication as Auth
from misc import printdoc

class AuthenticationTest(unittest.TestCase):
    """
    Freerange Authentication class tests.
    """
    def test_get_uri(self):
        """
        Validate authentication uri construction.
        """
        self.assert_(self.auth.uri == "auth", \
               "authentication URL was not properly constructed")
        
    @printdoc
    def test_authenticate(self):
        """
        Sanity check authentication method stub (lame).
        """
        self.assert_(self.auth.authenticate() == (None, None, None), \
               "authenticate() did not return a two-tuple")
        
    @printdoc
    def test_headers(self):
        """
        Ensure headers are being set.
        """
        self.assert_(self.auth.headers['x-auth-user'] == 'jsmith', \
               "storage user header not properly assigned")
        self.assert_(self.auth.headers['x-auth-key'] == 'xxxxxxxx', \
               "storage password header not properly assigned")
        
    def setUp(self):
        self.auth = Auth('jsmith', 'xxxxxxxx')
    def tearDown(self):
        del self.auth

# vim:set ai ts=4 tw=0 sw=4 expandtab:
