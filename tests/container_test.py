#!/usr/bin/python

import unittest
from freerange  import Connection, Object
from freerange.authentication import MockAuthentication as Auth
from fakehttp   import CustomHTTPConnection
from misc       import printdoc

class ContainerTest(unittest.TestCase):
    """
    Freerange Container class tests.
    """
    @printdoc
    def test_create_object(self):
        """
        Verify that Container.create_object() returns an Object instance.
        """
        storage_object = self.container.create_object('object1')
        assert isinstance(storage_object, Object)

    @printdoc
    def test_delete_object(self):
        """
        Simple sanity check of Container.delete_object()
        """
        self.container.delete_object('object1')

    @printdoc
    def test_get_object(self):
        """
        Verify that Container.get_object() returns an Object instance.
        """
        storage_object = self.container.get_object('object1')
        assert isinstance(storage_object, Object)

    @printdoc
    def test_get_objects(self):
        """
        Iterate an ObjectResults and verify that it returns Object instances.
        Validate that the count() and index() methods work as expected.
        """
        objects = self.container.get_objects()
        for storage_object in objects:
            assert isinstance(storage_object, Object)
        assert objects.count('object1') == 1
        assert objects.index('object3') == 2
        
    @printdoc
    def test_list_objects(self):
        """
        Verify that Container.list_objects() returns a list object.
        """
        assert isinstance(self.container.list_objects(), list)

    @printdoc
    def test_limited_list_objects(self):
        """
        Verify that query parameter passing works by passing a limit.
        """
        assert len(self.container.list_objects(limit=3)) == 3

    def setUp(self):
        self.auth = Auth('fakeaccount', 'jsmith', 'qwerty', 'http://localhost')
        self.conn = Connection(auth=self.auth)
        self.conn.conn_class = CustomHTTPConnection
        self.conn.http_connect()
        self.container = self.conn.get_container('container1')
    def tearDown(self):
        del self.container
        del self.conn
        del self.auth

# vim:set ai sw=4 ts=4 tw=0 expandtab:
