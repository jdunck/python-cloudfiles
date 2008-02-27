#!/usr/bin/python

import unittest
from misc       import printdoc
from fakehttp   import CustomHTTPConnection
from freerange  import Connection, Basket
from freerange.authentication import MockAuthentication as Auth

class ConnectionTest(unittest.TestCase):
    """
    Freerange Connection class tests.
    """
    @printdoc
    def test_create_basket(self):
        """
        Verify that Connection.create_basket() returns a Basket instance.
        """
        basket = self.conn.create_basket('basket1')
        assert isinstance(basket, Basket)

    @printdoc
    def test_delete_basket(self):
        """
        Simple sanity check of Connection.delete_basket()
        """
        self.conn.delete_basket('basket1')

    @printdoc
    def test_get_all_baskets(self):
        """
        Iterate a BasketResults and verify that it returns Basket instances.
        Validate that the count() and index() methods work as expected.
        """
        baskets = self.conn.get_all_baskets()
        for instance in baskets:
            assert isinstance(instance, Basket)
        assert baskets.count('basket1') == 1
        assert baskets.index('basket3') == 2

    @printdoc
    def test_get_basket(self):
        """
        Verify that Connection.get_basket() returns a Basket instance.
        """
        basket = self.conn.get_basket('basket1')
        assert isinstance(basket, Basket)

    @printdoc
    def test_list_baskets(self):
        """
        Verify that Connection.list_baskets() returns a list object.
        """
        assert isinstance(self.conn.list_baskets(), list)

    def setUp(self):
        self.auth = Auth('fakeaccount', 'jsmith', 'qwerty', 'http://localhost')
        self.conn = Connection(auth=self.auth)
        self.conn.conn_class = CustomHTTPConnection
        self.conn.http_connect()
    def tearDown(self):
        del self.conn
        del self.auth

# vim:set ai sw=4 ts=4 tw=0 expandtab:
