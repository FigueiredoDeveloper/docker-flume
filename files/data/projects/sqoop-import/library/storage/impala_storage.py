__author__ = 'rafael'

import re

import library
from library.storage import Driver
from impala import dbapi

class StoreEngine(Driver):
    """
    Oracle driver

    :param user: Oracle user
    :param pass: Oracle password
    :param conn_str: Oracle connection string
    """

    name = "impala"
    _cur = None

    def connect(self):
        assert "host" in self._attrs

        self.conn = dbapi.connect(
            **self._attrs
        )

        # Statement cursor
        self._cur = self.conn.cursor()

    def writer(self):
        lines = 0
        while not self._finished or not self._queue.empty():
            try:
                # Get the next item from queue to store it
                this_item = self._queue.get(block=True)

                query = "INSERT INTO {table} ({cols}) VALUES ({vals})".format(
                    table=this_item["dest"],
                    cols=",".join(this_item["cols"]),
                    vals=",".join(_quote(this_item["result_set"]))
                )

                if self._log:
                    self._log.debug(query)

                # This module does not implement connection checking tools
                try:
                    self._cur.execute(query)
                except Exception as e:
                    self.connect()
                    self._cur.execute(query)

                if lines % 1000 == 0:
                    self.conn.commit()
                lines += 1

            except Exception as e:
                if self._log:
                    self._log.error("Error saving result set to Impala. " + str(e) + " " + query)

        self.conn.commit()

def _quote(values):
    quoted = []
    for v in values:
        # Change ' to \'
        value = re.sub("'", "\\'", v)

        # Quote values
        if type(value) in [str, unicode]:
            if len(value) > 0 and value[0] == "'":
                quoted.append(value)
            else:
                quoted.append("'" + value + "'")
        else:
            quoted.append(value)

    return quoted


