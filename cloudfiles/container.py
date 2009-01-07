"""
container operations

Containers are storage compartments where you put your data (objects).
A container is similar to a directory or folder on a conventional filesystem
with the exception that they exist in a flat namespace, you can not create
containers inside of containers.

See COPYING for license information.
"""

from storage_object import Object, ObjectResults
from errors import ResponseError, InvalidContainerName, InvalidObjectName, \
                   ContainerNotPublic, CDNNotEnabled
from utils  import requires_name
from consts import default_cdn_ttl, container_name_limit
from fjson  import json_loads

# Because HTTPResponse objects *have* to have read() called on them 
# before they can be used again ...
# pylint: disable-msg=W0612

class Container(object):
    """
    Container object and Object instance factory.

    If your account has the feature enabled, containers can be publically
    shared over a global content delivery network.

    @ivar name: the container's name (generally treated as read-only)
    @type name: str
    @ivar object_count: the number of objects in this container (cached)
    @type object_count: number
    @ivar size_used: the sum of the sizes of all objects in this container
            (cached)
    @type size_used: number
    @ivar cdn_ttl: the time-to-live of the CDN's public cache of this container
            (cached, use make_public to alter)
    @type cdn_ttl: number
    """
    def __set_name(self, name):
        # slashes make for invalid names
        if isinstance(name, (str, unicode)) and \
                ('/' in name or len(name) > container_name_limit):
            raise InvalidContainerName(name)
        self._name = name

    name = property(fget=lambda self: self._name, fset=__set_name,
        doc="the name of the container (read-only)")

    def __init__(self, connection=None, name=None, count=None, size=None):
        """
        Containers will rarely if ever need to be instantiated directly by the
        user.

        Instead, use the L{create_container<Connection.create_container>},
        L{get_container<Connection.get_container>},
        L{list_containers<Connection.list_containers>} and
        other methods on a valid Connection object.
        """
        self._name = None
        self.name = name
        self.conn = connection
        self.object_count = count
        self.size_used = size
        self.cdn_uri = None
        self.cdn_ttl = None
        if connection.cdn_enabled:
            self._fetch_cdn_data()

    @requires_name(InvalidContainerName, container_name_limit)
    def _fetch_cdn_data(self):
        """
        Fetch the object's CDN data from the CDN service
        """
        response = self.conn.cdn_request('HEAD', [self.name])
        if (response.status >= 200) and (response.status < 300):
            for hdr in response.getheaders():
                if hdr[0].lower() == 'x-cdn-uri':
                    self.cdn_uri = hdr[1]
                if hdr[0].lower() == 'x-ttl':
                    self.cdn_ttl = int(hdr[1])

    @requires_name(InvalidContainerName, container_name_limit)
    def make_public(self, ttl=default_cdn_ttl):
        """
        Either publishes the current container to the CDN or updates its
        CDN attributes.  Requires CDN be enabled on the account.

        @param ttl: cache duration in seconds of the CDN server
        @type ttl: number
        """
        if not self.conn.cdn_enabled:
            raise CDNNotEnabled()
        if self.cdn_uri:
            request_method = 'POST'
        else:
            request_method = 'PUT'
        hdrs = {'X-TTL': str(ttl), 'X-CDN-Enabled': 'True'}
        response = self.conn.cdn_request(request_method, [self.name], hdrs=hdrs)
        if (response.status < 200) or (response.status >= 300):
            raise ResponseError(response.status, response.reason)
        self.cdn_ttl = ttl
        for hdr in response.getheaders():
            if hdr[0].lower() == 'x-cdn-uri':
                self.cdn_uri = hdr[1]

    @requires_name(InvalidContainerName, container_name_limit)
    def make_private(self):
        """
        Disables CDN access to this container.
        It may continue to be available until its TTL expires.
        """
        if not self.conn.cdn_enabled:
            raise CDNNotEnabled()
        hdrs = {'X-CDN-Enabled': 'False'}
        response = self.conn.cdn_request('POST', [self.name], hdrs=hdrs)
        if (response.status < 200) or (response.status >= 300):
            raise ResponseError(response.status, response.reason)

    def is_public(self):
        """
        Returns a boolean indicating whether or not this container is
        publically accessible via the CDN.
        @rtype: bool
        @return: whether or not this container is published to the CDN
        """
        if not self.conn.cdn_enabled:
            raise CDNNotEnabled()
        return self.cdn_uri is not None

    @requires_name(InvalidContainerName, container_name_limit)
    def public_uri(self):
        """
        Return the URI for this container, if it is publically
        accessible via the CDN.
        @rtype: str
        @return: the public URI for this container
        """
        if not self.is_public():
            raise ContainerNotPublic()
        return self.cdn_uri

    @requires_name(InvalidContainerName, container_name_limit)
    def create_object(self, object_name):
        """
        Return an L{Object} instance, creating it if necessary.
        
        When passed the name of an existing object, this method will 
        return an instance of that object, otherwise it will create a
        new one.

        @type object_name: str
        @param object_name: the name of the object to create
        @rtype: L{Object}
        @return: an object representing the newly created storage object
        """
        return Object(self, object_name)

    """
    order_by constants (can be either ascending or descending)

    create date/time
    name
    size
    content
    """
    OB_MODIFIED_ASC = 1
    OB_MODIFIED_DESC = 2
    OB_NAME_ASC = 3
    OB_NAME_DESC = 4
    OB_SIZE_ASC = 5
    OB_SIZE_DESC = 6
    OB_CONTENT_ASC = 7
    OB_CONTENT_DESC = 8
    ORDER_BY = {
        OB_MODIFIED_ASC: 'last_modified',
        OB_MODIFIED_DESC: 'last_modified desc',
        OB_NAME_ASC: 'name',
        OB_NAME_DESC: 'name desc',
        OB_BYTES_ASC: 'bytes',
        OB_BYTES_DESC: 'bytes desc',
        OB_CONTENT_ASC: 'content_type',
        OB_CONTENT_DESC: 'content_type desc'
        }

    @requires_name(InvalidContainerName, container_name_limit)
    def get_objects(self, prefix=None, limit=None, offset=None, 
                    order_by=None, path=None, **parms):
        """
        Return a result set of all Objects in the Container.
        
        Keyword arguments are treated as HTTP query parameters and can
        be used limit the result set (see the API documentation).

        @param prefix: filter the results using this prefix
        @type prefix: str
        @param limit: return the first "limit" objects found
        @type limit: int
        @param offset: return objects starting at "offset" in list
        @type offset: int
        @param order_by: order results by ????
        @type order_by: OB_MODIFIED_ASC: created date ascending
                        OB_MODIFIED_DESC: created date descending
                        OB_NAME_ASC: name ascending
                        OB_NAME_DESC: name descending
                        OB_BYTES_ASC: size ascending
                        OB_BYTES_DESC: size descending
                        OB_CONTENT_ASC: content type ascending
                        OB_CONTENT_DESC: content type descending
        @param path: return all objects in "path"
        @type path: str

        @rtype: L{ObjectResults}
        @return: an iterable collection of all storage objects in the container
        """
        return ObjectResults(self, self.list_objects_info(
                prefix, limit, offset, order_by, path, **parms))

    @requires_name(InvalidContainerName, container_name_limit)
    def get_object(self, object_name):
        """
        Return an Object instance for an existing storage object.
        
        If an object with a name matching object_name does not exist
        then a L{NoSuchObject} exception is raised.

        @param object_name: the name of the object to retrieve
        @type object_name: str
        @rtype: L{Object}
        @return: an Object representing the storage object requested
        """
        return Object(self, object_name, force_exists=True)

    @requires_name(InvalidContainerName, container_name_limit)
    def list_objects_info(self, prefix=None, limit=None, offset=None, 
                          order_by=None, path=None, **parms):
        """
        Return information about all Objects in the Container.
        
        Keyword arguments are treated as HTTP query parameters and can
        be used limit the result set (see the API documentation).

        @param prefix: filter the results using this prefix
        @type prefix: str
        @param limit: return the first "limit" objects found
        @type limit: int
        @param offset: return objects starting at "offset" in list
        @type offset: int
        @param order_by: order results by ????
        @type order_by: OB_MODIFIED_ASC: created date ascending
                        OB_MODIFIED_DESC: created date descending
                        OB_NAME_ASC: name ascending
                        OB_NAME_DESC: name descending
                        OB_BYTES_ASC: size ascending
                        OB_BYTES_DESC: size descending
                        OB_CONTENT_ASC: content type ascending
                        OB_CONTENT_DESC: content type descending
        @param path: return all objects in "path"
        @type path: str

        @rtype: list({"name":"...", "hash":..., "size":..., "type":...})
        @return: a list of all container info as dictionaries with the
                 keys "name", "hash", "size", and "type"
        """
        parms['format'] = 'json'
        resp = self._list_objects_raw(
            prefix, limit, offset, order_by, path, **parms)
        return json_loads(resp)

    @requires_name(InvalidContainerName, container_name_limit)
    def list_objects(self, prefix=None, limit=None, offset=None, 
                     order_by=None, path=None, **parms):
        """
        Return names of all Objects in the Container.
        
        Keyword arguments are treated as HTTP query parameters and can
        be used limit the result set (see the API documentation).

        @param prefix: filter the results using this prefix
        @type prefix: str
        @param limit: return the first "limit" objects found
        @type limit: int
        @param offset: return objects starting at "offset" in list
        @type offset: int
        @param order_by: order results by ????
        @type order_by: OB_MODIFIED_ASC: created date ascending
                        OB_MODIFIED_DESC: created date descending
                        OB_NAME_ASC: name ascending
                        OB_NAME_DESC: name descending
                        OB_BYTES_ASC: size ascending
                        OB_BYTES_DESC: size descending
                        OB_CONTENT_ASC: content type ascending
                        OB_CONTENT_DESC: content type descending
        @param path: return all objects in "path"
        @type path: str

        @rtype: list(str)
        @return: a list of all container names
        """
        resp = self._list_objects_raw(
            prefix, limit, offset, order_by, path, **parms)
        return resp.splitlines()

    @requires_name(InvalidContainerName, container_name_limit)
    def _list_objects_raw(self, prefix=None, limit=None, offset=None, 
                     order_by=None, path=None, **parms):
        """
        Returns a chunk list of storage object info.
        """
        if prefix: parms['prefix'] = prefix
        if limit: parms['limit'] = limit
        if offset: parms['offset'] = offset
        if order_by: parms['order_by'] = Container.ORDER_BY[order_by]
        if path: parms['path'] = path
        response = self.conn.make_request('GET', [self.name], parms=parms)
        if (response.status < 200) or (response.status > 299):
            buff = response.read()
            raise ResponseError(response.status, response.reason)
        return response.read()

    def __getitem__(self, key):
        return self.get_object(key)

    def __str__(self):
        return self.name

    @requires_name(InvalidContainerName, container_name_limit)
    def delete_object(self, object_name):
        """
        Permanently remove a storage object.
        
        @param object_name: the name of the object to retrieve
        @type object_name: str
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

    This class implements dictionary- and list-like interfaces.
    """
    def __init__(self, conn, containers=list()):
        self._containers = containers
        self._names = [k['name'] for k in containers]
        self.conn = conn

    def __getitem__(self, key):
        return Container(self.conn,
                         self._containers[key]['name'], 
                         self._containers[key]['count'], 
                         self._containers[key]['bytes'])

    def __getslice__(self, i, j):
        return [Container(self.conn, k['name'], k['count'], k['size']) for k in self._containers[i:j] ]

    def __contains__(self, item):
        return item in self._names

    def __repr__(self):
        return 'ContainerResults: %s containers' % len(self._containers)
    __str__ = __repr__
    
    def __len__(self):
        return len(self._containers)

    def index(self, value, *args):
        """
        returns an integer for the first index of value
        """
        return self._names.index(value, *args)

    def count(self, value):
        """
        returns the number of occurrences of value
        """
        return self._names.count(value)

# vim:set ai sw=4 ts=4 tw=0 expandtab:
