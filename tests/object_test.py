#!/usr/bin/python

import unittest, md5
from freerange         import Object, Connection
from freerange.errors  import ResponseError, InvalidObjectName
from freerange.authentication import MockAuthentication as Auth
from fakehttp          import CustomHTTPConnection
from misc              import printdoc
from tempfile          import mktemp
import os

class ObjectTest(unittest.TestCase):
    """
    Freerange Object class tests.
    """
    @printdoc
    def test_read(self):
        """
        Test an Object's ability to read.
        """
        assert "teapot" in self.storage_object.read()

    @printdoc
    def test_read_pass_headers(self):
        """
        Test an Object's ability to read when it has 
        extra headers passed in.
        """
        hdrs = {}
        hdrs['x-bogus-header-1'] = 'bogus value'
        hdrs['x-bogus-header-2'] = 'boguser value'
        assert "teapot" in self.storage_object.read(hdrs=hdrs)

    @printdoc
    def test_response_error(self):
        """
        Verify that reading a non-existent Object raises a ResponseError
        exception.
        """
        storage_object = self.container.create_object('bogus')
        self.assertRaises(ResponseError, storage_object.read)

    @printdoc
    def test_write(self):
        """
        Simple sanity test of Object.write()
        """
        self.storage_object.write('the rain in spain ...')

    @printdoc
    def test_sync_metadata(self):
        """
        Sanity check of Object.sync_metadata()
        """
        self.storage_object.metadata['unit'] = 'test'
        self.storage_object.sync_metadata()

    @printdoc
    def test_load_from_file(self):
        """
        Simple sanity test of Object.load_from_file().
        """
        path = os.path.join(os.path.dirname(__file__), 'samplefile.txt')
        self.storage_object.load_from_filename(path)
        
    @printdoc
    def test_save_to_filename(self):
        """Sanity test of Object.save_to_filename()."""
        tmpnam = mktemp()
        self.storage_object.save_to_filename(tmpnam)
        rdr = open(tmpnam, 'r')
        try:
            assert rdr.read() == self.storage_object.read(), \
                   "save_to_filename() stored invalid content!"
        finally:
            rdr.close()
            os.unlink(tmpnam)

    @printdoc
    def test_compute_md5sum(self):
        """
        Verify that the Object.compute_md5sum() class method returns an 
        accurate md5 sum value.
        """
        f = open('/bin/ls', 'r')
        m = md5.new()
        m.update(f.read())
        sum1 = m.hexdigest()
        f.seek(0)
        try:
            sum2 = Object.compute_md5sum(f)
            assert sum1 == sum2, "%s != %s" % (sum1, sum2)
        finally:
            f.close()
            
    @printdoc
    def test_bad_name(self):
        """
        Ensure you can't assign an invalid object name.
        """
        obj = Object(self.container)    # name is None
        self.assertRaises(InvalidObjectName, obj.read)
        self.assertRaises(InvalidObjectName, obj.stream)
        self.assertRaises(InvalidObjectName, obj.sync_metadata)
        self.assertRaises(InvalidObjectName, obj.write, '')
        
        obj.name = ''    # name is zero-length string
        self.assertRaises(InvalidObjectName, obj.read)
        self.assertRaises(InvalidObjectName, obj.stream)
        self.assertRaises(InvalidObjectName, obj.sync_metadata)
        self.assertRaises(InvalidObjectName, obj.write, '')
        
    def setUp(self):
        self.auth = Auth('fakeaccount', 'jsmith', 'qwerty', 'http://localhost')
        self.conn = Connection(auth=self.auth)
        self.conn.conn_class = CustomHTTPConnection
        self.conn.http_connect()
        self.container = self.conn.get_container('container1')
        self.storage_object = self.container.get_object('object1')
    def tearDown(self):
        del self.storage_object
        del self.container
        del self.conn
        del self.auth


# vim:set ai sw=4 ts=4 tw=0 expandtab:
