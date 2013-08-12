import logging
from beaker.exceptions import InvalidCacheBackendError

from beaker_extensions.nosql import Container
from beaker_extensions.nosql import NoSqlManager

try:
    from hypertable.thriftclient import *
    from hyperthrift.gen.ttypes import *
except ImportError:
    raise InvalidCacheBackendError("Hypertable cache backend requires the 'hypertable' library")

log = logging.getLogger(__name__)

class HypertableManager(NoSqlManager):
    def __init__(self, namespace, url=None, data_dir=None, lock_dir=None, **params):
        NoSqlManager.__init__(self, namespace, url=url, data_dir=data_dir, lock_dir=lock_dir, **params)

    def open_connection(self, host, port):
        self.client = ThriftClient(str(host), int(port))
        self.namespace = self.client.namespace_open('readible')

    def __contains__(self, key):
        self.client.get_cell(self.namespace, 'beaker_cache', key, 'session').exists()

    def set_value(self, key, value):
        cells = [ [key,'session',"",value] ]
        self.client.set_cells_as_arrays(self.namespace, 'beaker_cache', cells)

    def __getitem__(self, key):
        return self.client.get_cell(self.namespace, 'beaker_cache', key, 'session')

    def __delitem__(self, key):
        delquery="delete * from beaker_cache where row = '"+key+"'"
        self.client.hql_query(self.namespace, delquery)

    def _format_key(self, key):
        return 'beaker:%s:%s' % (self.namespace, key.replace(' ', '\302\267'))

    def do_remove(self):
        raise Exception("Unimplemented")

    def keys(self):
        raise Exception("Unimplemented")


class HypertableContainer(Container):
    namespace_class = HypertableManager
