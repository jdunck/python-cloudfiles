
"""
basket operations

Baskets are containers where you put your data (eggs). A basket is
similar to a directory or folder on a conventional filesystem with the
exception that they exist in a flat namespace, you can not create
baskets inside of baskets.
"""

from egg        import Egg, EggResults
from errors     import ResponseError

# Because HTTPResponse objects *have* to have read() called on them 
# before they can be used again ...
# pylint: disable-msg=W0612

class Basket(object):
    """
    Basket object and Egg instance factory.
    """
    def __init__(self, connection=None, name=None, count=None, size=None):
        self.name = name
        self.conn = connection
        self.egg_count = count
        self.size_used = size

    def create_egg(self, egg_name):
        """
        Returns a new Egg instance.
        """
        return Egg(self, egg_name)

    def get_eggs(self, **parms):
        """
        Return a result set of all eggs in the basket.
        """
        return EggResults(self, self.list_eggs(**parms))

    def get_egg(self, egg_name):
        """
        Given an egg name, return a corresponding Egg object.
        """
        return Egg(self, egg_name, force_exists=True)

    def list_eggs(self, **parms):
        """
        Returns a list of eggs.
        """
        response = self.conn.make_request('GET', [self.name], parms=parms)
        if (response.status < 200) or (response.status > 299):
            raise ResponseError(response.status, response.reason)
        return response.read().splitlines()

    def __getitem__(self, key):
        return self.get_egg(key)

    def __str__(self):
        return self.name

    def delete_egg(self, egg_name):
        """
        Permanently remove an egg.
        """
        if isinstance(egg_name, Egg):
            egg_name = egg_name.name
        response = self.conn.make_request('DELETE', [self.name, egg_name])
        if (response.status < 200) or (response.status > 299):
            raise ResponseError(response.status, response.reason)
        buff = response.read()

class BasketResults(object):
    """
    An iterable results set object for Baskets. 
    """
    def __init__(self, conn, baskets=list()):
        self._baskets = baskets
        self.conn = conn

    def __getitem__(self, key):
        return Basket(self.conn, self._baskets[key])

    def __getslice__(self, i, j):
        return [Basket(self._baskets, k) for k in self._baskets[i:j]]

    def __contains__(self, item):
        return item in self._baskets

    def __repr__(self):
        return repr(self._baskets)

    def __len__(self):
        return len(self._baskets)

    def index(self, value, *args):
        """
        returns an integer for the first index of value
        """
        return self._baskets.index(value, *args)

    def count(self, value):
        """
        returns the number of occurrences of value
        """
        return self._baskets.count(value)

# vim:set ai sw=4 ts=4 tw=0 expandtab:
