__author__ = 'rafael'

import sys
import json
import time
from pyspark import SparkContext, HiveContext, SparkConf

import library
from library.storage import Driver

class StoreEngine(Driver):
    """
    Spark Store engine

    :param conf: Spark configuration (optional)
    :param defaults: Loads Spark default configuration, usually with `spark-submit` (optional)
    :param batch_size: Max items per batch (RDD) (optional)
    """

    name = "spark"
    _spark_job_name = None
    _spark_conf = None
    _spark_context = None
    _spark_batch_size = None
    _hive_context = None
    _isFinished = None
    _stop_wait_time = None
    _convert_types = None

    # This dict will have one key per "dest"
    # Each key is a list of items got from storage queue
    # It allow us to store into multiples destinations
    # into the same storage instance
    _data = {}

    # Thread safe semaphore

    # Helper to convert raw data to spark compatible types
    # Default: str
    _spark_type = {
        "BooleanType": bool,
        "ByteType": bytes,
        "DoubleType": float,
        "FloatType": float,
        "IntegerType": int,
        "LongType": long,
        "ShortType": int,
    }

    def __init__(self, log=None, default_batch_size=1024*1024*5, stop_wait_time=100, convert_types=True, **kwargs):
        Driver.__init__(self, log=log, **kwargs)

        # Assert time to wait until stop to receive data
        self._stop_wait_time = stop_wait_time

        # Assert batch_size (bytes) default: 5MB
        self._spark_batch_size = default_batch_size

        # Convert data types?
        self._convert_types = convert_types

        # Assert spark job configuration
        # SparkConf
        if kwargs.has_key("conf"):
            self._spark_conf = kwargs["conf"]
        else:
            self._spark_conf = SparkConf()

        # Spark job name
        if kwargs.has_key("job_name"):
            self._spark_job_name = kwargs["job_name"]
        else:
            self._spark_job_name = "Storage " + self.name

    def connect(self):
        self._debug("Connecting to " + self.name)

        if self._spark_conf:
            self._spark_context = SparkContext(conf=self._spark_conf)
        else:
            self._spark_context = SparkContext(
                "local[4]",
                self._spark_job_name
            )
        self._hive_context = HiveContext(self._spark_context)

    def connected(self):
        if self._spark_context is None or self._hive_context is None:
            return False

        return True

    def writer(self):
        # Buffer counter
        lines = 0

        this_item = None
        self._isFinished = False
        while not self._finished or not self._queue.empty():
            try:
                # Get the next item from queue to store it
                try:
                    this_item = self._queue.get(block=True, timeout=5)

                except Exception as e:
                    self._debug("Queue timeout" + str(e))
                    continue

                dest = this_item["dest"]

                if not self.connected():
                    self.connect()

                if not self._data.has_key(dest):
                    self._debug("Creating place to " + dest)
                    self._data[dest] = {
                        "data": [],
                        "schema": self._hive_context.table(dest).schema,
                        "locked": False
                    }

                    # Generate FiledName -> Type for data conversion
                    if self._convert_types:
                        self._data[dest]["field_type"] = {}
                        for field in self._data[dest]["schema"].fields:
                            self._data[dest]["field_type"][field.name] = field.dataType

                    # Prepare ware for partitined inserts
                    if this_item.has_key("partition_by"):
                        self._hive_context.setConf('hive.exec.dynamic.partition.mode', 'nonstrict')
                        self._data[dest]["partition_by"] = this_item["partition_by"]
                    self._debug("Metadata " + dest + ": " + str(self._data[dest]))


                while self._data[dest]["locked"]:
                    time.sleep(0.001)

                if hasattr(this_item, "cols"):
                    # Table / CSV format conversion to dict
                    this_item["result_set"] = dict(zip(
                        this_item["cols"], # Keys
                        this_item["result_set"] # Values
                    ))

                if self._convert_types:
                    this_item["result_set"] = self._convert_data_types(
                        dest,
                        this_item["result_set"]
                    )

                # Dict format (raw)
                if type(this_item["result_set"]) is list:
                    # Multiple data entries
                    self._data[dest]["data"].extend(this_item["result_set"])
                else:
                    # Single data entry
                    self._data[dest]["data"].append(this_item["result_set"])

                # Flush data to Spark
                if sys.getsizeof(self._data[dest]["data"]) >= self._spark_batch_size:
                    self._debug("Flushing data")
                    self._flush_table(dest)
                lines += 1

            except Exception as e:
                msg = "Error saving result set to " + self.name + ": " + str(e)
                if this_item:
                    msg += " " + str(this_item)
                self._error(msg)

        self._debug("Final flush")
        self._flush()
        return

    def _flush(self):

        for dest in self._data:
            try:
                self._flush_table(dest)

            except Exception as e:
                self._debug("Spark error: " + str(e))

    def _flush_table(self, dest):
        # Lock writer to avoid multithread data writting colisions
        self._debug("Flusher locked for " + dest)
        self._data[dest]["locked"] = True

        try:
            self._debug("+ DATA " + dest)

            _rdd = self._spark_context.parallelize([
                json.dumps(self._data[dest]["data"])
            ])

            self._debug("Creating data frame for " + dest)
            _df = self._hive_context.jsonRDD(
                _rdd,
                self._data[dest]["schema"]
            )

            if self._data[dest].has_key("partition_by"):
                _df.repartition(1).write.partitionBy(
                    self._data[dest]["partition_by"]
                ).insertInto(dest)

            else:
                _df.repartition(1).write.insertInto(dest)

            self._data[dest]["data"] = []
            self._debug("Finished table " + dest)

        except Exception as e:
            self._debug("Spark error: " + str(e))

        self._data[dest]["locked"] = False
        self._debug("Flusher released")

    def finish(self):
        if sys.version_info >= (2,7):
            #Driver(self).finish()
            super(StoreEngine, self).finish()
        else:
            # Python <= 2.6 GO HORSE
            self._finished = True

        self._debug("Waiting to stop receiving data")
        time.sleep(self._stop_wait_time)

        self.flush()
        self._isFinished = True

    def flush(self):
        # Write data of each table
        self._debug("Writing data to the database")
        self._flush()
        self._debug("Wrote all table to the database")

    def getConnection(self):
        if not self.connected():
            self.connect()

        return {
            "spark_context": self._spark_context,
            "hive_context": self._hive_context
        } 

    def isFinished(self):
        return self._isFinished

    def _convert_data_types(self, dest, data):
        self._debug("Converting data types")
        if type(data) is list:
            # Convert list of items
            for i in range(len(data)):
                for k in data[i].keys():
                    if k in self._data[dest]["schema"].names:
                        if self._data[dest]["field_type"].has_key(k):
                            data[i][k] = self._convert(
                                self._data[dest]["field_type"][k],
                                data[i][k]
                            )
                        if data[k] is None:
                            del(data[k])
                    else:
                       del(data[i][k])

        else:
            # Convert unique item
            for k in data.keys():
                if k in self._data[dest]["schema"].names:
                    data[k] = self._convert(
                        self._data[dest]["field_type"][k],
                        data[k]
                    )
                    if data[k] is None:
                        del(data[k])
                else:
                   del(data[k])

        self._debug("Converted data " + dest + ":" + json.dumps(data))
        return data

    def _convert(self, data_type, data):
        if data is None:
            return

        if self._spark_type.has_key(str(data_type)):
            self._debug(str(data) + ":" + str(data_type) + ":" + str(self._spark_type[str(data_type)]))
            return self._spark_type[str(data_type)](data)

        else:
            self._debug(str(data) + ":" + str(data_type) + ":str")
            return str(data)

