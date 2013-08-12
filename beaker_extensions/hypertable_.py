import logging
from beaker.exceptions import InvalidCacheBackendError

from beaker_extensions.nosql import Container
from beaker_extensions.nosql import NoSqlManager

try:
    from hypertable.thriftclient import *
    from hyperthrift.gen.ttypes import *
    import json
except ImportError:
    raise InvalidCacheBackendError("Hypertable cache backend requires the 'hypertable' library")

log = logging.getLogger(__name__)

class HypertableManager(NoSqlManager):
    def __init__(self, namespace, url=None, data_dir=None, lock_dir=None, **params):
        print "init", "namespace", namespace
        NoSqlManager.__init__(self, namespace, url=url, data_dir=data_dir, lock_dir=lock_dir, **params)

    def open_connection(self, host, port):
        self.client = ThriftClient(str(host), int(port))
        self.ns = self.client.namespace_open('readible')

    def __contains__(self, key):
        print "contains", "key", key
        try:
            value=self.client.get_cell(self.ns, 'beaker_cache', self._format_key(key), 'session')
            print "contains", "value", value
            return True
        except Exception, e:
            print "contains", "exception", e
            return False

    def set_value(self, key, value):
        print "set_value", "key", key
        cells = [ [self._format_key(key),'session',"",json.dumps(value)] ]
        self.client.set_cells_as_arrays(self.ns, 'beaker_cache', cells)

    def __getitem__(self, key):
        print "getitem", "key", key
        try:
            return json.loads(self.client.get_cell(self.ns, 'beaker_cache', self._format_key(key), 'session'))
        except Exception,e:
            return None

    def __delitem__(self, key):
        delquery="delete * from beaker_cache where row = '"+self._format_key(key)+"'"
        self.client.hql_query(self.ns, delquery)

    def _format_key(self, key):
        print "format_key", "key", key
        print "self.namespace", self.namespace
        return 'beaker:%s:%s' % (self.namespace, key.replace(' ', '\302\267'))

    def do_remove(self):
        raise Exception("Unimplemented")

    def keys(self):
        raise Exception("Unimplemented")


class HypertableContainer(Container):
    namespace_class = HypertableManager
