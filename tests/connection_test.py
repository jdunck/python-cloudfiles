#!/usr/bin/python

import unittest
from misc       import printdoc
from fakehttp   import CustomHTTPConnection
from cloudfiles import Connection, Container
from cloudfiles.authentication import MockAuthentication as Auth
from cloudfiles.errors import InvalidContainerName

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
        self.assert_(isinstance(container, Container))

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
            self.assert_(isinstance(instance, Container))
        self.assert_(containers.count('container1') == 1)
        self.assert_(containers.index('container3') == 2)

    @printdoc
    def test_get_container(self):
        """
        Verify that Connection.get_container() returns a Container instance.
        """
        container = self.conn.get_container('container1')
        self.assert_(isinstance(container, Container))

    @printdoc
    def test_list_containers(self):
        """
        Verify that Connection.list_containers() returns a list object.
        """
        self.assert_(isinstance(self.conn.list_containers(), list))

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

    @printdoc
    def test_account_info(self):
        """
        Test to see if the account has only one container
        """
        #container = self.conn.get_container('container1')
        print self.conn.get_info()
        self.assert_(self.conn.get_info()[0] == 1)

    def setUp(self):
        self.auth = Auth('jsmith', 'qwerty')
        self.conn = Connection(auth=self.auth)
        self.conn.conn_class = CustomHTTPConnection
        self.conn.http_connect()
    def tearDown(self):
        del self.conn
        del self.auth

# vim:set ai sw=4 ts=4 tw=0 expandtab:
