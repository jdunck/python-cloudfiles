
import unittest
from misc             import printdoc
from freerange.utils  import parse_url

@printdoc
def test_parse_url():
    """
    Validate that the parse_url() function properly returns the hostname, 
    port number, path (if any), and ssl boolean. Attempts several 
    different URL permutations, (5 tests total).
    """
    urls = {
        'http_noport_nopath': {
            'url':   'http://bogus.not',
            'host':  'bogus.not',
            'port':  80,
            'path':  '',
            'ssl':   False,
        },
        'https_noport_nopath': {
            'url':   'https://bogus.not',
            'host':  'bogus.not',
            'port':  443,
            'path':  '',
            'ssl':   True,
        },
        'http_noport_withpath': {
            'url':   'http://bogus.not/v1/bar',
            'host':  'bogus.not',
            'port':  80,
            'path':  'v1/bar',
            'ssl':   False,
        },
        'http_withport_nopath': {
            'url':   'http://bogus.not:8000',
            'host':  'bogus.not',
            'port':  8000,
            'path':  '',
            'ssl':   False,
        },
        'https_withport_withpath': {
            'url':   'https://bogus.not:8443/v1/foo',
            'host':  'bogus.not',
            'port':  8443,
            'path':  'v1/foo',
            'ssl':   True,
        },
    }
    for url in urls:
        yield check_url, url, urls[url]

def check_url(test, urlspec):
    (host, port, path, ssl) = parse_url(urlspec['url'])
    assert host == urlspec['host'], "%s failed on host assertion" % test
    assert port == urlspec['port'], "%s failed on port assertion" % test
    assert path == urlspec['path'], "%s failed on path assertion" % test
    assert ssl == urlspec['ssl'], "%s failed on ssl assertion" % test

# vim:set ai sw=4 ts=4 tw=0 expandtab:
