__author__ = 'rafael'

import time
import importlib
import threading
from sys import stderr
from Queue import Queue


class Store:
    """
    Class to save data to multiple data storage systems
    One queue and one thread are opened to each storage
    system to write the data.
    The data is replicated into all the systems.

    Eg.: Save data to a Oracle table and to a HDFS CSV like file
    from library.storage import Store

    info = {
        "oracle": {
            "conn_str": "(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.13.11.26)(PORT=1521)))(CONNECT_DATA=(SID=BWTKCPR1)))",
            "user": "my_user",
            "pass": "my_pass"
        },
        "hdfs": {
            "url": "http://namenode.hadoop.b2w:50070",
            "user": "hdfs",
            "root": "/tmp"
        }
    }

    store = Store(store_info=info, log=log)
    store.start()

    # Table / CSV like data example
    store.put({
                # Table / HDFS full file name
                "dest": "atualizadata",
                "result_set": ["'01-JAN-85'", "'15-DEC-85'"],
                "cols": ["DATAINICIO", "DATAFIM"]
    })

    # Object data format
    store.put({
                # Hive table
                "dest": "database_name.atualizadata",
                "result_set": {
                    "DATAINICIO": "'01-JAN-85'",
                    "DATAFIM": "'15-DEC-85'"
                }
    })

    store.finish()
    """

    # Storage handlers
    _storage = {}

    def __init__(self, store_info, log=None):
        """
        Constructor

        :param store_info: Connection info for each store system (see each *_storage.py for the correct usage)
        :param log: Log handler object
        """

        self._store_info = store_info
        self._log = log

    def start(self):
        """
        Start each storage system

        """

        for store in self._store_info:
            # Load storage module
            mod = importlib.import_module("library.storage." + store + "_storage")

            # Init the driver for this storage
            store_class = mod.StoreEngine
            self._storage[store] = store_class(
                self._log,
                **self._store_info[store]
            )

    def put(self, data, **kwargs):
        """
        Put data to each storage system queue

        :param data: Data to be inserted/load
        :param kwargs: Attributes forwarded to 'put' Queue module
        """

        assert type(data) is dict
        assert "dest" in data
        assert "result_set" in data

        for store in self._storage:
            self._storage[store].put(data, **kwargs)

    def finish(self, block=True):
        """
        Inform to each storage system that all data was already passed to them

        """

        if hasattr(self, "log"):
            self.log.debug("Finishing storages...")

        for store in self._storage:
            if hasattr(self, "log"):
                self.log.debug("Finishing " + store)
            self._storage[store].finish()

        if hasattr(self, "log"):
            self.log.debug("Storages finished")

        if(block):
            if hasattr(self, "log"):
                self.log.debug("Waiting")
            while not self.isFinished():
                time.sleep(0.1)

    def getConnections(self):
        """
        Return the connections to each storage systems

        """
        connections = {}
        for store in self._storage:
            try:
                connections[store] = self._storage[store].getConnection()
            except Exception as e:
                if self._log:
                    self._log.error("getConnection() error at " + store + ". " + str(e))
        return connections

    def isFinished(self):
        for store in self._storage:
            if not self._storage[store].isFinished():
                return False
        return True

class Driver(object):
    """
    Abstract class for defining the others drivers modules for the storage systems

    """

    # Connection object
    conn = None

    # Flag to indicate that all data where sent to us
    _finished = False

    # Driver type
    name = None

    def __init__(self, log=None, queue_max_size=0, **kwargs):
        """
        Constructor
        Starts the queues and threads

        :param log: Log handler object
        :param queue_max_size: Max size of the Queue
        :param kwargs: Attributes forwarded to StoreEngine drivers class
        """

#        super(object, self).__init__()
        self._log = log
        self._attrs = kwargs
        self._queue_max_size = queue_max_size
        self._queue = Queue(maxsize=self._queue_max_size)
        self._worker = threading.Thread(name=self.name, target=self.writer).start()

    def put(self, data):
        """
        Append data to the queue for writing

        :param data: Data to store (list)
        :return: Forward the 'put' status
        """

        return self._queue.put(data)

    def connected(self):
        """
        Check connection status

        :return: Boolean
        """

        pass

    def connect(self):
        """
        Connect to the storage system

        """

        pass

    def getConnection(self):
        """
        Retrieves the storage system connection object

        """

        pass

    def writer(self):
        """
        Listen to the Queue and write the resultsets
        to the storage system

        """

        pass

    def finish(self):
        """
        Inform data read concluded

        """
        self._finished = True

    def _debug(self, msg):
        if self._log:
            self._log.debug(msg)
        else:
            print msg

    def _error(self, msg):
        if self._log:
            self._log.error(msg)
        else:
            print >>stderr, msg
