
"""
Remote storage client API.

Working with result sets

>> import freerange
>> conn = freeragen.get_connection(account="Acme", username='jsmith', 
..     password="supersekrit", authurl="http://fowl.racklabs.com")
>> baskets = conn.get_all_baskets()
>> type(baskets)
<class 'freerange.basket.BasketResults'>
>> len(baskets)
2
>> for basket in baskets:
>>     print basket.name
fruit
vegitables
>> print basket[0].name
fruit
>> fruit_basket = basket[0]
>> eggs = fruit_basket.get_all_eggs()
>> for egg in eggs:
>>     print egg.name
apple
orange
bannana
>>

Creating baskets and adding eggs to them

>> easter_basket = conn.create_basket('easter')
>> purple_egg = easter_basket['purple']
>> purple_egg.load_from_file('images/easter/purple.jpg')
>> pink_egg = easter_basket['pink']
>> f = open('images/easter/pink.gif', 'rb')
>> pink_egg.write(f)
>> bad_egg = easter_basket['bad']
>> bad_egg.write('This is not the egg you are looking for.')
>> bad_egg.read()
'This is not the egg you are looking for.'
"""

from freerange.connection  import Connection, ConnectionPool
from freerange.basket      import Basket
from freerange.egg         import Egg

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
