__author__ = 'rafael'

from library import hdfs_client
from library.storage import Driver


class StoreEngine(Driver):
    """
    HDFS driver

    :param add_header: Flag to put or not column name to the file (Boolean)
    :param by passed to 'hdfs_client.Client' (eg. url, user, root, etc.)
    """

    name = "hdfs"
    _wrote_cols = {}
    _add_header = False
    _over_write = False

    def connect(self):
        assert "root" in self._attrs
        self.conn = hdfs_client.Client(log=self._log, **self._attrs)

    def connected(self):
        if self.conn is None:
            return False

        try:
            self.conn.status(self._attrs["root"])
            return True

        except Exception as e:
            if self._log:
                self._log.warning("Lost connection do HDFS " + str(e))
            return False

    def writer(self):
        # Storage additional options
        if "add_header" in self._attrs:
            self._add_header = self._attrs["add_header"]
            del self._attrs["add_header"]

        if "over_write" in self._attrs:
            self._over_write = self._attrs["over_write"]
            del self._attrs["over_write"]

        while not self._finished or not self._queue.empty():
            this_item = None
            try:
                # Get the next item from queue to store it
                this_item = self._queue.get(block=True)
                self._insert(this_item)

            except Exception as e:
                if self._log:
                    self._log.error("Error saving result set to HDFS. " + str(this_item["result_set"]) + ":" + str(e))

                # Retry
                if this_item:
                    self._insert(this_item)

    def _insert(self, item):

        if self._log:
            self._log.debug(str(item))

        if not self.connected():
            self.connect()

        # Write header col names if necessary
        if self._add_header and "cols" in item and item["dest"] not in self._wrote_cols:
            try:
                if self._over_write and self.conn.status(item["dest"]):
                    self.conn.delete(item["dest"])
            except Exception as e:
                if self._log:
                    self._log.debug(item["dest"] + " not found to overwrite. That is ok! Continuing...")

            self.conn.append(
                item["dest"],
                item["cols"]
            )
            self._wrote_cols[item["dest"]] = True

        # Write data
        self.conn.append(
            item["dest"],
            item["result_set"]
        )
