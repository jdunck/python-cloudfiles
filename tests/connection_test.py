#!/usr/bin/python

import unittest
from misc       import printdoc
from fakehttp   import CustomHTTPConnection
from freerange  import Connection, Container
from freerange.authentication import MockAuthentication as Auth
from freerange.errors import InvalidContainerName

class ConnectionTest(unittest.TestCase):
    """
    Freerange Connection class tests.
    """
    @printdoc
    def test_create_container(self):
        """
        Verify that Connection.create_container() returns a Container instance.
        """
        container = self.conn.create_container('container1')
        assert isinstance(container, Container)

    @printdoc
    def test_delete_container(self):
        """
        Simple sanity check of Connection.delete_container()
        """
        self.conn.delete_container('container1')

    @printdoc
    def test_get_all_containers(self):
        """
        Iterate a ContainerResults and verify that it returns Container instances.
        Validate that the count() and index() methods work as expected.
        """
        containers = self.conn.get_all_containers()
        for instance in containers:
            assert isinstance(instance, Container)
        assert containers.count('container1') == 1
        assert containers.index('container3') == 2

    @printdoc
    def test_get_container(self):
        """
        Verify that Connection.get_container() returns a Container instance.
        """
        container = self.conn.get_container('container1')
        assert isinstance(container, Container)

    @printdoc
    def test_list_containers(self):
        """
        Verify that Connection.list_containers() returns a list object.
        """
        assert isinstance(self.conn.list_containers(), list)

    @printdoc
    def test_bad_names(self):
        """
        Verify that methods do not accept invalid container names.
        """
        exccls = InvalidContainerName
        for badname in ('', 'yougivelove/abadname'):
            self.assertRaises(exccls, self.conn.create_container, badname)
            self.assertRaises(exccls, self.conn.get_container, badname)
            self.assertRaises(exccls, self.conn.delete_container, badname)
        
    def setUp(self):
        self.auth = Auth('fakeaccount', 'jsmith', 'qwerty', 'http://localhost')
        self.conn = Connection(auth=self.auth)
        self.conn.conn_class = CustomHTTPConnection
        self.conn.http_connect()
    def tearDown(self):
        del self.conn
        del self.auth

# vim:set ai sw=4 ts=4 tw=0 expandtab:
