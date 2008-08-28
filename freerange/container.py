
"""
container operations

Containers are storage compartments where you put your data (objects).
A container is similar to a directory or folder on a conventional filesystem
with the exception that they exist in a flat namespace, you can not create
containers inside of containers.

See COPYING for license information.
"""

from storage_object import Object, ObjectResults
from errors import ResponseError, InvalidContainerName, InvalidObjectName
from utils import requires_name

# Because HTTPResponse objects *have* to have read() called on them 
# before they can be used again ...
# pylint: disable-msg=W0612

class Container(object):
    """
    Container object and Object instance factory.
    """
    def __set_name(self, name):
        # slashes make for invalid names
        if isinstance(name, (str, unicode)) and '/' in name:
            raise InvalidContainerName(name)
        self._name = name
        
    name = property(fget=lambda self: self._name, fset=__set_name)
        
    def __init__(self, connection=None, name=None, count=None, size=None):
        self._name = None
        self.name = name
        self.conn = connection
        self.object_count = count
        self.size_used = size

    @requires_name(InvalidContainerName)
    def create_object(self, object_name):
        """
        Return an Object instance, creating it if necessary.
        
        Arguments:
        object_name -- the name of the object to create
        
        When passed the name of an existing object, this method will 
        return an instance of that object, otherwise it will create a
        new one.
        """
        return Object(self, object_name)

    @requires_name(InvalidContainerName)
    def get_objects(self, **parms):
        """
        Return a result set of all Objects in the Container.
        
        Keyword arguments are treated as HTTP query parameters and can
        be used limit the result set (see the API documentation).
        """
        return ObjectResults(self, self.list_objects(**parms))

    @requires_name(InvalidContainerName)
    def get_object(self, object_name):
        """
        Return an Object instance for an existing storage object.
        
        Arguments:
        object_name -- the name of the object to retrieve

        If an object with a name matching object_name does not exist
        then a NoSuchObject exception is raised.
        """
        return Object(self, object_name, force_exists=True)

    @requires_name(InvalidContainerName)
    def list_objects(self, **parms):
        """
        Returns a list of storage object names.
        
        Keyword arguments are treated as HTTP query parameters and can
        be used limit the result set (see the API documentation).
        """
        response = self.conn.make_request('GET', [self.name], parms=parms)
        if (response.status < 200) or (response.status > 299):
            buff = response.read()
            raise ResponseError(response.status, response.reason)
        return response.read().splitlines()

    def __getitem__(self, key):
        return self.get_object(key)

    def __str__(self):
        return self.name

    @requires_name(InvalidContainerName)
    def delete_object(self, object_name):
        """
        Permanently remove a storage object.
        
        Arguments:
        object_name -- the name of the object to retrieve
        """
        if isinstance(object_name, Object):
            object_name = object_name.name
        if not object_name:
            raise InvalidObjectName(object_name)
        response = self.conn.make_request('DELETE', [self.name, object_name])
        if (response.status < 200) or (response.status > 299):
            buff = response.read()
            raise ResponseError(response.status, response.reason)
        buff = response.read()

class ContainerResults(object):
    """
    An iterable results set object for Containers. 
    """
    def __init__(self, conn, containers=list()):
        self._containers = containers
        self.conn = conn

    def __getitem__(self, key):
        return Container(self.conn, self._containers[key])

    def __getslice__(self, i, j):
        return [Container(self._containers, k) for k in self._containers[i:j]]

    def __contains__(self, item):
        return item in self._containers

    def __repr__(self):
        return repr(self._containers)

    def __len__(self):
        return len(self._containers)

    def index(self, value, *args):
        """
        returns an integer for the first index of value
        """
        return self._containers.index(value, *args)

    def count(self, value):
        """
        returns the number of occurrences of value
        """
        return self._containers.count(value)

# vim:set ai sw=4 ts=4 tw=0 expandtab:
