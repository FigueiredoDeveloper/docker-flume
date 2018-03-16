# -*- coding: utf-8 -*-
"""
This module provides facilities to use the HDFS

"""

import csv
import cStringIO
from hdfs import TokenClient

# CSV quote levels
QUOTE_NONE = csv.QUOTE_NONE
QUOTE_MINIMAL = csv.QUOTE_MINIMAL
QUOTE_NUMERIC = csv.QUOTE_NONNUMERIC
QUOTE_ALL = csv.QUOTE_ALL


class Client(TokenClient):
    _log = None
    _csv = None
    _csv_args = {}
    _stream = None
    _formatted_data = None

    def __init__(self, url, user, root, log=None, **kwargs):
        """
        Facility to handle files into HDFS

        :param url: HDFS address (eg.: http://namenode.hadoop.b2w:50070)
        :param user: Login name to pass to HDFS
        :param root: Root HDFS directory to use as CWD
        :param log: Logger object
        :param kwargs: Extra attributes handled by `hdfs.TokenClient` class
        :return:
        """

        self._log = log

        # Set the hdfs user
        if "params" not in kwargs:
            kwargs["params"] = {"user.name": user}
        if "user.name" not in kwargs["params"]:
            kwargs["params"]["user.name"] = user

        # Work Directory
        kwargs["root"] = root

        if self._log:
            self._log.debug("HDFS params: " + str(kwargs))

        # Connect to HDFS
        if self._log:
            self._log.debug("Starting HDFS handler object...")

        try:
            TokenClient.__init__(self, url, user, **kwargs)

        except Exception as e:
            if self._log:
                self._log.error("Failed to connect to HDFS " + str(e))
            raise e

        if self._log:
            self._log.debug("HDFS connection OK")

    def set_csv(self, **kwargs):
        """
        Set attributes for CSV stream to be handled into the file

        :param kwargs: Attributes passed to CSV handler
        :return:
        """

        self._stream = cStringIO.StringIO()
        self._csv_args = kwargs

        if self._log:
            self._log.debug("CSV args: " + str(self._csv_args))

        self._csv = csv.writer(
            self._stream,
            **self._csv_args
        )

    def _to_string(self, data):
        """
        Transform object data to string, respecting its type.
        Eg.: Lists are converted to CSV with `set_csv()` method
        parameters

        :param data: Data to be converted
        :return: String
        """

        if type(data) is list:
            # Prepare CSV processor
            if self._csv is None or self._stream.closed:
                self.set_csv(**self._csv_args)

            # Format data as CSV
            self._csv.writerow(data)
            self._formatted_data = self._stream.getvalue()
            self._stream.close()
        else:
            self._formatted_data = str(data)

    def append(self, path, data, **kwargs):
        """
        Creates and/or append to files on HDFS
        Fist we try to append to an existing file,
        if this file does not exist, we attempt
        to create them.

        :param path: File name on HDFS passed the hdfs module
        :param data: Data to append to file (dict, list and str)
        :param kwargs: All the stuff handled by the hdfs module
        :return: void
        """

        # Try to append to previously existing file (unless `overwrite`)
        if "append" not in kwargs and \
                ("overwrite" not in kwargs or kwargs["overwrite"] is False):
            kwargs["append"] = True

        self._to_string(data)

        try:
            self.write(path, self._formatted_data, **kwargs)

        except Exception as e:
            if self._log:
                self._log.debug("Retrying HDFS error " + str(e))

            # Generally it's error is related to 'File not found for appending'
            kwargs["append"] = False

            try:
                self.write(path, self._formatted_data, **kwargs)
            except Exception as e:
                if self._log:
                    self._log.error("HDFS write error! " + str(e))
                raise e
