import logging
from beaker.exceptions import InvalidCacheBackendError

from beaker_extensions.nosql import Container
from beaker_extensions.nosql import NoSqlManager

try:
    import rawes
except ImportError:
    raise InvalidCacheBackendError("Elastic Search cache backend requires the 'rawes' library")

log = logging.getLogger(__name__)

class ElasticSearchManager(NoSqlManager):
    '''
    Elastic Search Python client packages data in JSON by default.
    '''
    def __init__(self, namespace, url=None, data_dir=None, lock_dir=None, **params):
        NoSqlManager.__init__(self, namespace, url=url, data_dir=data_dir, lock_dir=lock_dir, **params)

    def open_connection(self, host, port):
        self.es = rawes.Elastic('thrift://' + host + ":" + str(port))

    def __contains__(self, key):
        print "contains"
        try:
            print "contains - try"
            session=self.es.get('beaker_cache/session/' + self._format_key(key))
        except Exception:
            print "contains - exception"
            return False
        if session!=None and session['exists']:
            print "contains - if"
            return True
        else:
            print "contains - else"
            return False

    def set_value(self, key, value):   
        print "set_value"
        try:
            print "set_value - try"
            session=self.es.get('beaker_cache/session/' + self._format_key(key))
            print "set_value - before post"
            self.es.post('beaker_cache/session/'+self._format_key(key)+"/_update", data=value)
            print "set_value - after post"
        except Exception:
            print "set_value - exception"
            self.es.put('beaker_cache/session/'+self._format_key(key), data=value)
            print "set_value - after exception"

    def __getitem__(self, key):
        try:
            print "getitem try"
            item = self.es.get('beaker_cache/session/' + self._format_key(key))['_source']
            print "getitem returning"
            return item
        except Exception,e:
            print "getitem exception"
            return None

    def __delitem__(self, key):
        try:
            self.es.delete('beaker_cache/session/'+self._format_key(key))
        except Exception, e:
            print "error deleting session"

    def _format_key(self, key):
        return 'beaker:%s:%s' % (self.namespace, key.replace(' ', '\302\267'))

    def do_remove(self):
        raise Exception("Unimplemented")

    def keys(self):
        raise Exception("Unimplemented")


class ElasticSearchContainer(Container):
    namespace_class = ElasticSearchManager
