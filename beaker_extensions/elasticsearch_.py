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
        try:
            session=self.es.get('beaker_cache/session/' + self._format_key(key))
        except Exception:
            return False
        if session!=None and session['exists']:
            return True
        else:
            return False

    def set_value(self, key, value):        
        try:
            session=self.es.get('beaker_cache/session/' + self._format_key(key))
            self.es.post('beaker_cache/session/'+self._format_key(key)+"/_update", data=value)
        except Exception:
            self.es.put('beaker_cache/session/'+self._format_key(key), data=value)

    def __getitem__(self, key):
        return self.es.get('beaker_cache/session/' + self._format_key(key))['_source']

    def __delitem__(self, key):        
        self.es.delete('beaker_cache/session/'+self._format_key(key))

    def _format_key(self, key):
        return 'beaker:%s:%s' % (self.namespace, key.replace(' ', '\302\267'))

    def do_remove(self):
        raise Exception("Unimplemented")

    def keys(self):
        raise Exception("Unimplemented")


class ElasticSearchContainer(Container):
    namespace_class = ElasticSearchManager
