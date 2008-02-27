
"""
exception classes
"""

class ResponseError(Exception):
    """
    Raised when the remote service returns an error.
    """
    def __init__(self, status, reason):
        self.status = status
        self.reason = reason
        Exception.__init__(self)

    def __str__(self):
        return '%d: %s' % (self.status, self.reason)

    def __repr__(self):
        return '%d: %s' % (self.status, self.reason)

class NoSuchBasket(Exception):
    """
    Raised on a non-existent basket.
    """
    pass

class NoSuchEgg(Exception):
    """
    Raised on a non-existent egg.
    """
    pass

class BasketNotEmpty(Exception):
    """
    Raised when attempting to delete a basket that still contains eggs.
    """
    def __init__(self, basket_name):
        self.basket_name = basket_name
        
    def __str__(self):
        return "Cannot delete non-empty basket %s" % self.basket_name
    
    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, self.basket_name)

class InvalidUrl(Exception):
    """
    Not a valid url for use with this software.
    """
    pass