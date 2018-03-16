import time
from kafka import SimpleProducer, KafkaClient
from kafka.common import LeaderNotAvailableError, ProduceResponse

class Connection:
    __free = True
    __kafka = None
    __producer = None
    __log = None
    __async = None
    __response = None
    __topic = None
    __connection_ttl = None
    __last_connection_time = None

    def __init__(self, hosts, retry_limit=1000, retry_interval=0.01, log=None, async=False, connection_ttl=None):
        """
        Kafka connection handler object

        :param hosts: List of host:port of the kafka cluster
        :param retry_limit: Max retries to send the messages
        :param retry_interval: Time (in seconds) to wait before reconnect
        :param log: Logger object to debug the execution
        :param async: Kafka producer async mode
        :param connection_ttl: Connection's Time To Live. Required by some environments (Atlas)
        """
        self.__counter_messages = 0
        self.__counter_connects = 0
        self.__hosts = hosts
        self.__retry_limit = retry_limit
        self.__retry_interval = retry_interval
        self.__log = log
        self.__async = async
        self.__connection_ttl = connection_ttl

    def is_free(self, status=None):
        """
        Connection semaphore checking (thread safe)

        :param status: Update the 'free' status
        :return:
        """
        if status is not None:
            self.__free = status

        return self.__free

    def connect(self):
        """
        Connecto to the kafka cluster

        :return:
        """
        self.disconnect()

        if self.__log:
            self.__log.debug("Connecting to kafka")

        try:
            self.__kafka = KafkaClient(self.__hosts, timeout=self.__connection_ttl or 120)
            self.__producer = SimpleProducer(self.__kafka, async=self.__async)

        except Exception as e:
            if self.__log:
                self.__log.error("Failed to connect to kafka " + str(e))

        if self.__log:
            self.__log.debug("K:" + str(type(self.__kafka)) + " P:" + str(type(self.__producer)))

        if self.__producer is None:
            if self.__log:
                self.__log.error("Failed to connect to kafka " + str(e))
        else:
            self.__counter_connects += 1
            self.__last_connection_time = time.time()

    def disconnect(self):
        """
        Close the kafka connection

        :return:
        """
        if self.__kafka is not None:
            self.__kafka.close()
            time.sleep(self.__retry_interval)

    def send_data(self, topic, message):
        """
        Write messages to the Kafka cluster

        :param topic: Kafka topic
        :param message: Message
        :return:
        """
        self.__topic = topic.encode("utf-8")

        tries = int(self.__retry_limit)
        while tries:
            tries -= 1
            try:
                if self.__log:
                    self.__log.debug("Trying to write to kafka + " + str(self.__retry_limit - tries))

                if type(self.__producer) is not SimpleProducer:
                    if self.__log:
                        self.__log.debug("Lost producer. Reconnecting...")
                    self.connect()

                # Programmed reconnect to Kafka due to some environments issues (Atlas)
                if self.__connection_ttl and self.__last_connection_time + self.__connection_ttl < time.time():
                    self.connect()

                self.__response = self.__producer.send_messages(self.__topic, bytes(message))
                self.__counter_messages += 1
                if self.__response[0].error == 0:
                    if self.__log:
                        self.__log.debug("Kafka write success")
                    return self.__response

            except LeaderNotAvailableError:
                if self.__log:
                    self.__log.warning("Kafka LeaderNotAvailableError " + e.message)
                time.sleep(self.__retry_interval)
                self.__producer = None

            except IndexError as ie:
                """ This error doesn't means problems writing message """
                return self.__producer

            except Exception as e:
                if self.__log:
                    self.__log.warning("Kafka write error. Try #" + str(self.__retry_limit - tries) + ". " + e.message)
                time.sleep(self.__retry_interval)
                self.__producer = None

        if self.__log:
            self.__log.error("Kafka write error. Max tries")

    def success(self):
        """
        Check for Kafka write error.

        ATTENTION: Async mode does not allow to track producer errors
        :return: Boolean
        """
        try:
            if type(self.__response) is list:
                if len(self.__response) >= 1:

                    # Async post (driver doesn't return status)
                    if type(self.__response[0]) is SimpleProducer or type(self.__response[0]) is ProduceResponse:
                        return True

                    return hasattr(self.__response[0], "error") and self.__response[0] == 0
                else:
                    return False

        except Exception as e:
            if self.__log:
                self.__log.error("Error getting message status. " + str(e))

            return False

    def counter_messages(self):
        """
        Number of messagens sent to the kafka cluster

        :return: Integer
        """
        return self.__counter_messages

    def counter_connects(self):
        """
        Number of [re]connects on the Kafka cluster

        :return: Integer
        """
        return self.__counter_connects


class Pool:
    __log = None
    __connectionPool = []

    def __init__(self, connections_pool_size, log=None, **kwargs):
        self.__log = log

        for i in range(connections_pool_size):
            if self.__log:
                self.__log.info("Starting connection %s..." % i)

            this_con = Connection(
                log=self.__log,
                **kwargs
            )
            self.__connectionPool.append(this_con)

    def stats(self):
        status = []
        for conn in self.__connectionPool:
            status.append({
                'messages': conn.counter_messages(),
                'connects': conn.counter_connects()
            })

        return status

    def put(self, topic, message):
        while True:
            for conn in self.__connectionPool:
                if conn.is_free():
                    conn.is_free(False)

                    if self.__log:
                        self.__log.debug("Sending message...")

                    kafka_status = conn.send_data(topic, message)
                    conn.is_free(True)

                    if conn.success():
                        if self.__log:
                            self.__log.info("Message sent.")

                        return True

                    else:
                        if self.__log:
                            self.__log.error("Message not sent! " + str(kafka_status))

            time.sleep(0.001)
