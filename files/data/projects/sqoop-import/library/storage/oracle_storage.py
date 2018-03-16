__author__ = 'rafael'

import re

import library
from library.storage import Driver


class StoreEngine(Driver):
    """
    Oracle driver

    :param user: Oracle user
    :param pass: Oracle password
    :param conn_str: Oracle connection string
    """

    name = "oracle"
    _cur = None
    _isFinished = None
    _base_query = "INSERT INTO {table} ({cols}) VALUES ({vals})"

    def connect(self):
        self.conn = library.oracle_connect(
            self._attrs["user"],
            self._attrs["pass"],
            self._attrs["conn_str"]
        )

        # Statement cursor
        self._cur = self.conn.cursor()

    def connected(self):
        if self.conn is None:
            return False

        return self.conn.ping()

    def writer(self):
        lines = 0
        query = ""
        self._isFinished = False
        while not self._finished or not self._queue.empty():
            try:
                # Get the next item from queue to store it
                this_item = self._queue.get(block=True)

                if this_item.has_key("cols"):
                    # Table / CSV format conversion to dict
                    query = self._base_query.format(
                        table=this_item["dest"],
                        cols=",".join(this_item["cols"]),
                        vals=",".join(_quote(this_item["result_set"]))
                    )
                else:
                    # Dict format (raw)
                    if type(this_item["result_set"]) is list:
                        query = ""
                        for d in this_item["result_set"]:
                            print "1" + str( d)
                            query += self._base_query.format(
                                table=this_item["dest"],
                                cols=",".join(d.keys()),
                                vals=",".join(_quote(d.values()))
                            )
                            self._cur.execute(query)

                        self.conn.commit()
                    else:
                        print "2" + str( this_item["result_set"])
                        query = self._base_query.format(
                            table=this_item["dest"],
                            cols=",".join(this_item["result_set"].keys()),
                            vals=",".join(_quote(this_item["result_set"].values()))
                        )
     
                if self._log:
                    self._log.debug(self.name + ": " + query)

                if not self.connected():
                    self.connect()

                self._cur.execute(query)

                self.conn.commit()
                lines += 1

            except Exception as e:
                if self._log:
                    self._log.error("Error saving result set to Oracle. " + str(e) + " " + query)

        self.conn.commit()
        self._isFinished = True

    def getConnection(self):
        if not self.connected():
            self.connect()
        return self.conn

    def isFinished(self):
        return self._isFinished

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

