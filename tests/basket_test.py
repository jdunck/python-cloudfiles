#!/usr/bin/python

import unittest
from freerange  import Connection, Egg
from freerange.authentication import MockAuthentication as Auth
from fakehttp   import CustomHTTPConnection
from misc       import printdoc

class BasketTest(unittest.TestCase):
    """
    Freerange Basket class tests.
    """
    @printdoc
    def test_create_egg(self):
        """
        Verify that Basket.create_egg() returns an Egg instance.
        """
        egg = self.basket.create_egg('eggggg1')
        assert isinstance(egg, Egg)

    @printdoc
    def test_delete_egg(self):
        """
        Simple sanity check of Basket.delete_egg()
        """
        self.basket.delete_egg('eggggg1')

    @printdoc
    def test_get_egg(self):
        """
        Verify that Basket.get_egg() returns an Egg instance.
        """
        egg = self.basket.get_egg('eggggg1')
        assert isinstance(egg, Egg)

    @printdoc
    def test_get_eggs(self):
        """
        Iterate an EggResults and verify that it returns Egg instances.
        Validate that the count() and index() methods work as expected.
        """
        eggs = self.basket.get_eggs()
        for egg in eggs:
            assert isinstance(egg, Egg)
        assert eggs.count('eggggg1') == 1
        assert eggs.index('eggggg3') == 2
        
    @printdoc
    def test_list_eggs(self):
        """
        Verify that Basket.list_eggs() returns a list object.
        """
        assert isinstance(self.basket.list_eggs(), list)

    @printdoc
    def test_limited_list_eggs(self):
        """
        Verify that query parameter passing works by passing a limit.
        """
        assert len(self.basket.list_eggs(limit=3)) == 3

    def setUp(self):
        self.auth = Auth('fakeaccount', 'jsmith', 'qwerty', 'http://localhost')
        self.conn = Connection(auth=self.auth)
        self.conn.conn_class = CustomHTTPConnection
        self.conn.http_connect()
        self.basket = self.conn.get_basket('basket1')
    def tearDown(self):
        del self.basket
        del self.conn
        del self.auth

# vim:set ai sw=4 ts=4 tw=0 expandtab:
