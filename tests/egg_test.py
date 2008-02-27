#!/usr/bin/python

import unittest, md5
from freerange         import Egg, Connection
from freerange.errors  import ResponseError
from freerange.authentication import MockAuthentication as Auth
from fakehttp          import CustomHTTPConnection
from misc              import printdoc
import os

class EggTest(unittest.TestCase):
    """
    Freerange Egg class tests.
    """
    @printdoc
    def test_read(self):
        """
        Test an eggs ability to read.
        """
        assert "teapot" in self.egg.read()

    @printdoc
    def test_response_error(self):
        """
        Verify that reading a non-existent egg raises a ResponseError
        exception.
        """
        egg = self.basket.create_egg('bogus')
        self.assertRaises(ResponseError, egg.read)

    @printdoc
    def test_write(self):
        """
        Simple sanity test of Egg.write()
        """
        self.egg.write('the rain in spain ...')

    @printdoc
    def test_sync_metadata(self):
        """
        Sanity check of Egg.sync_metadata()
        """
        self.egg.metadata['unit'] = 'test'
        self.egg.sync_metadata()

    @printdoc
    def test_load_from_file(self):
        """
        Simple sanity test of Egg.load_from_file().
        """
        self.egg.load_from_filename(os.path.dirname(__file__) \
                + '/../tests/samplefile.txt')

    @printdoc
    def test_compute_md5sum(self):
        """
        Verify that the Egg.compute_md5sum() class method returns an 
        accurate md5 sum value.
        """
        f = open('/bin/ls', 'r')
        m = md5.new()
        m.update(f.read())
        sum1 = m.hexdigest()
        f.seek(0)
        try:
            sum2 = Egg.compute_md5sum(f)
            assert sum1 == sum2, "%s != %s" % (sum1, sum2)
        finally:
            f.close()

    def setUp(self):
        self.auth = Auth('fakeaccount', 'jsmith', 'qwerty', 'http://localhost')
        self.conn = Connection(auth=self.auth)
        self.conn.conn_class = CustomHTTPConnection
        self.conn.http_connect()
        self.basket = self.conn.get_basket('basket1')
        self.egg = self.basket.get_egg('eggggg1')
    def tearDown(self):
        del self.egg
        del self.basket
        del self.conn
        del self.auth


# vim:set ai sw=4 ts=4 tw=0 expandtab:
