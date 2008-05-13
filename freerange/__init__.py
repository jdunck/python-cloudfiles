
"""
Remote storage client API.

Working with result sets

>> import freerange
>> conn = freeragen.get_connection(account="Acme", username='jsmith', 
..     password="supersekrit", authurl="http://fowl.racklabs.com")
>> containers = conn.get_all_containers()
>> type(containers)
<class 'freerange.container.ContainerResults'>
>> len(containers)
2
>> for container in containers:
>>     print container.name
fruit
vegitables
>> print container[0].name
fruit
>> fruit_container = container[0]
>> objects = fruit_container.get_all_objects()
>> for storage_object in objects:
>>     print storage_object.name
apple
orange
bannana
>>

Creating Containers and adding Objects to them

>> pic_container = conn.create_container('pictures')
>> my_dog = pic_container.create_object('fido.jpg')
>> my_dog.load_from_file('images/IMG-0234.jpg')

>> text_obj = pic_container.create_object('sample.txt')
>> text_obj.write('This is not the object you are looking for.\\n')
>> text_obj.read()
'This is not the object you are looking for.'

Object instances support streaming through the use of a generator.

>> deb_iso = pic_container.get_object('debian-40r3-i386-netinst.iso')
>> f = open('/tmp/debian.iso', 'w')
>> for chunk in deb_iso.stream():
..     f.write(chunk)
>> f.close()
"""

from freerange.connection     import Connection, ConnectionPool
from freerange.container      import Container
from freerange.storage_object import Object

def get_connection(*args, **kwargs):
    """
    Helper function for creating connection instances.
    """
    return Connection(*args, **kwargs)

def get_connection_pool(*args, **kwargs):
    """
    Helper function for creating connection pool instances.
    """
    return ConnectionPool(*args, **kwargs)
