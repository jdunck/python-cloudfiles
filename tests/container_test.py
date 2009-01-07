#!/usr/bin/python

import unittest
from cloudfiles  import Connection, Container, Object
from cloudfiles.authentication import MockAuthentication as Auth
from cloudfiles.errors import InvalidContainerName, InvalidObjectName
from cloudfiles.consts import container_name_limit
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
        self.assert_(isinstance(storage_object, Object))

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
        self.assert_(isinstance(storage_object, Object))

    @printdoc
    def test_get_objects(self):
        """
        Iterate an ObjectResults and verify that it returns Object instances.
        Validate that the count() and index() methods work as expected.
        """
        objects = self.container.get_objects()
        for storage_object in objects:
            self.assert_(isinstance(storage_object, Object))
        self.assert_(objects.count('object1') == 1)
        self.assert_(objects.index('object3') == 2)

    @printdoc
    def test_get_objects_parametrized(self):
        """
        Iterate an ObjectResults and verify that it returns Object instances.
        Validate that the count() and index() methods work as expected.
        """
        objects = self.container.get_objects(prefix='object', limit=3, offset=3, 
                                             order_by=Container.OB_NAME_ASC, 
                                             path='/')
        for storage_object in objects:
            self.assert_(isinstance(storage_object, Object))
        self.assert_(objects.count('object4') == 1)
        self.assert_(objects.index('object6') == 2)

    @printdoc
    def test_list_objects_info(self):
        """
        Verify that Container.list_objects_info() returns a list object.
        """
        self.assert_(isinstance(self.container.list_objects(), list))
        
    @printdoc
    def test_list_objects(self):
        """
        Verify that Container.list_objects() returns a list object.
        """
        self.assert_(isinstance(self.container.list_objects(), list))

    @printdoc
    def test_list_objects_limited(self):
        """
        Verify that limit & order query parameters work.
        """
        self.assert_(len(self.container.list_objects(limit=3)) == 3)
        self.assert_(len(self.container.list_objects(limit=3, offset=3)) == 3)

    @printdoc
    def test_list_objects_prefixed(self):
        """
        Verify that the prefix query parameter works.
        """
        self.assert_(isinstance(
                self.container.list_objects(prefix='object'), list))

    @printdoc
    def test_list_objects_path(self):
        """
        Verify that the prefix query parameter works.
        """
        self.assert_(isinstance(
                self.container.list_objects(path='/'), list))

    @printdoc
    def test_list_objects_ordered(self):
        """
        Verify that all the various order_by query paramete permutations work.
        """
        self.assert_(isinstance(self.container.list_objects(
                    order_by=Container.OB_MODIFIED_ASC), list))
        self.assert_(isinstance(self.container.list_objects(
                    order_by=Container.OB_MODIFIED_DESC), list))
        self.assert_(isinstance(self.container.list_objects(
                    order_by=Container.OB_NAME_ASC), list))
        self.assert_(isinstance(self.container.list_objects(
                    order_by=Container.OB_NAME_DESC), list))
        self.assert_(isinstance(self.container.list_objects(
                    order_by=Container.OB_BYTES_ASC), list))
        self.assert_(isinstance(self.container.list_objects(
                    order_by=Container.OB_BYTES_DESC), list))
        self.assert_(isinstance(self.container.list_objects(
                    order_by=Container.OB_CONTENT_ASC), list))
        self.assert_(isinstance(self.container.list_objects(
                    order_by=Container.OB_CONTENT_DESC), list))
        
    @printdoc
    def test_bad_name_assignment(self):
        """
        Ensure you can't assign an invalid name.
        """
        basket = Container(self.conn) 
        try:
            basket.name = 'yougivelove/abadname'
            self.fail("InvalidContainerName exception not raised!")
        except InvalidContainerName:
            pass

        try:
            basket.name = 'a'*(container_name_limit+1)
            self.fail("InvalidContainerName exception not raised!")
        except InvalidContainerName:
            pass

    @printdoc
    def test_bad_object_name(self):
        """
        Verify that methods do not accept invalid container names.
        """
        self.assertRaises(InvalidObjectName, self.container.delete_object, '')
            
    def setUp(self):
        self.auth = Auth('jsmith', 'qwerty')
        self.conn = Connection(auth=self.auth)
        self.conn.conn_class = CustomHTTPConnection
        self.conn.http_connect()
        self.container = self.conn.get_container('container1')
    def tearDown(self):
        del self.container
        del self.conn
        del self.auth

# vim:set ai sw=4 ts=4 tw=0 expandtab:
