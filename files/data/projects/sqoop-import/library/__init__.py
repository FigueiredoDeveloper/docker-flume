# -*- coding: utf-8 -*-
"""
This module provides facilities for params, log, db connection, api calls, etc.
"""

import os
import re
import sys
import json
import time
import yaml
import logging
from collections import Mapping
from argparse import ArgumentParser

# JSON error handler
json_decoder_errmsg_original = json.decoder.errmsg


class GetArgs(ArgumentParser):
    """
    Fast setup command line argument

    Eg.:
    ARGS_DEF = {
        'description': 'Crawler de Importacao Oracle',
    }
    ARGS_LIST = [
        [
            [
                '-c',
                '--config_file'
            ],
            {
                'required': True,
                'help': 'nome do arquivo de configuracao'
            }
        ],
        [
            '--mock',
            {
                'required': False,
                'help': 'Indica se esta rodando no modo mock'
            }
        ]
    ]
    args = GetArgs(ARGS_DEF, ARGS_LIST)
    args.parse_args()

    """

    def __init__(self, attrs=None, args=[]):
        ArgumentParser.__init__(self, **attrs)
        for arg in args:
            if type(arg[0]) == list:
                self.add_argument(*arg[0], **arg[1])
            elif type(arg[0]) == str:
                self.add_argument(arg[0], **arg[1])


class Log(logging.Logger):
    """
    Facility for writing log files
    This class creates a logging.Logger instance and attach a StreamHandler to it, writing to a predefined stream

    """

    # Log levels
    CRITICAL = logging.CRITICAL
    FATAL = logging.FATAL
    ERROR = logging.ERROR
    WARNING = logging.WARNING
    WARN = logging.WARN
    INFO = logging.INFO
    DEBUG = logging.DEBUG
    NOTSET = logging.NOTSET

    _handler = None
    _old_level = None
    _last_messages = []
    _last_messages_limit = 200

    def __init__(
            self,
            stream=sys.stderr,
            level=INFO,
            fmt='%(asctime)s - %(name)s (%(thread)d) - %(levelname)s - %(message)s',
            name=__file__
    ):
        """
        Log constructor

        :param stream: Data stream to write to
        :param level: Log level (same as logging module)
        :param fmt: Log format
        """

        logging.Logger.__init__(self, name, level=level)

        # Open stream for writing
        if type(stream) is str or type(stream) is unicode:
            # Create a log file
            self._handler = logging.StreamHandler(
                open(stream, mode='a')
            )
        else:
            # Log to 'stream'
            self._handler = logging.StreamHandler(stream)

        # Store level for use when leave debug mode
        self._old_level = level

        # Set formatter
        if type(fmt) is dict:
            formatter = logging.Formatter(**fmt)
        else:
            formatter = logging.Formatter(fmt)
        formatter.converter = time.gmtime
        self._handler.setFormatter(
            formatter
        )

        # Set log handler
        self.addHandler(self._handler)

    def set_debug(self, status=True):
        """
        Switch debug mode on/off

        :param status: Debug mode (boolean)
        """

        if status:
            self.setLevel(self.DEBUG)
        else:
            self.setLevel(self._old_level)

    def error(self, msg, *args, **kwargs):
        self._buffer(msg)
        if sys.version_info >= (2,7):
            return super(Log, self).error(msg, *args, **kwargs)
        else:
            return logging.Logger.error(self, msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self._buffer(msg)
        if sys.version_info >= (2,7):
            return super(Log, self).info(msg, *args, **kwargs)
        else:
            return logging.Logger.info(self, msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self._buffer(msg)
        if sys.version_info >= (2,7):
            return super(Log, self).warning(msg, *args, **kwargs)
        else:
            return logging.Logger.warning(self, msg, *args, **kwargs)

    def _buffer(self, msg):
        if len(self._last_messages) >= self._last_messages_limit:
            del self._last_messages[0]
        self._last_messages.append(msg)

    def get_last_messages(self):
        return self._last_messages


class B2W(Log):
    conf = None
    args = None

    def __init__(self, name=None, config_file=None, args_def=None, args_list=None):
        _filename = os.path.basename(sys.argv[0])
        _base_path = os.path.dirname(os.path.abspath(sys.argv[0]))

        if name is None:
            name = re.sub('\.py$', '', _filename)

        # Log handler
        log_file = "%s/logs/%s.log" % (_base_path, name)
        Log.__init__(self, name=name, stream=log_file)

        # Config file
        self.conf = init_conf(self)

        # Command Line Arguments
        if args_def or args_list:
            self.args = GetArgs(args_def, args_list).parse_args()

        # Set debug mode
        if hasattr(self.args, "debug"):
            self.set_debug(self.args.debug)


def oracle_connect(user, passwd, conn_str):
    from library import oracle_driver
    return oracle_driver.oracle_connect(user, passwd, conn_str)


def load_json(stream):
    """
    Convert the given JSON stream to object
    Threat the input stream to avoid JSON errors

    :param stream: File handler or stream with JSON
    :return: Object
    """

    output = None

    # Remove bad JSON chars
    if type(stream) is file:
        output = ''.join(l.replace('\r', ' ').replace('\n', ' ').replace('\t', ' ') for l in stream)
    else:
        output = str(stream).replace('\r', ' ').replace('\n', ' ').replace('\t', ' ')

    try:
        data = json.loads(output)
        return data

    except ValueError as e:
        # Could not parse data to JSON
        raise e

    except Exception as e:
        msg = "Error parsing JSON data! %s %s" % (stream, str(e))
        raise Exception(msg)


def init_log(name=__name__, **kwargs):
    """
    Initialize log tool

    :return: Log tool (object)
    """

    # Generate logfile into the script's directory
    base_path = os.path.dirname(os.path.abspath(sys.argv[0]))
    log_file = "%s/logs/%s.log" % (base_path, name)

    try:
        log = Log(name=name, stream=log_file, **kwargs)

    except Exception as e:
        print >> sys.stderr, 'Error creating logging object manipulator! %s' % str(e)
        return False

    return log


def init_conf(log=None, config_file=None):
    """
    Read the crawler configuration files

    :param log: Logging object
    :param config_file: File to read from
    :return: Project configuration (object)
    """

    conf = None
    try:
        # Get default configuration file (/<PROJECT_DIR>/conf/config.yml)
        if not config_file:
            config_file = "%s/conf/config.yml" % os.path.dirname(os.path.abspath(sys.argv[0]))

        # Load conf
        conf_obj = yaml.load(open(config_file, 'r'))
        conf = conf_obj

    except Exception as e:
        msg = "Error loading config file! %s" % str(e)
        if log:
            log.error(msg)
        raise Exception(msg)

    return conf


def merge(data1, data2, depth=-1):
    """
    Recursively merge or update dict-like objects.
    >>> merge({'k1': {'k2': 2}}, {'k1': {'k2': {'k3': 3}}, 'k4': 4})
    {'k1': {'k2': {'k3': 3}}, 'k4': 4}

    :param data1: First object to merge
    :param data2: Second object to merge
    :return: data1 and data2 merged (complex object)
    """

    for k, v in data2.iteritems():
        if isinstance(v, Mapping) and not depth == 0:
            r = merge(data1.get(k, {}), v, depth=max(depth - 1, -1))
            data1[k] = r
        elif isinstance(data1, Mapping):
            data1[k] = data2[k]
        else:
            data1 = {k: data2[k]}
    return data1


def json_handle_error(msg, doc, pos, end=None):
    json.last_error_position = json.decoder.linecol(doc, pos)

    msg = """Bad JSON data near -->%s""" % doc[pos-20:pos+20]
    log = init_log()
    log.error(msg)
    return json_decoder_errmsg_original(msg, doc, pos, end)


def jsonFlatten(jn, key=None):
    """
    Recebe um json (dict) e opcionalmente uma chave

    :param jn: Complex data
    :param key: For recursion only
    :return: Dict list

    """

    result = [{}] # sera uma lista de dicts

    if len(jn) > 0:
        for k, v in jn.iteritems():
            if key is None:
                newKey = k
            else:
                newKey = key + "_" + k

            parcialResult = []
            # vlist eh uma lista de jsons
            if isinstance(v, dict):
                # se v eh json
                vlist = jsonFlatten(v, newKey)
            elif isinstance(v, list):
                # se v eh array
                vlist = jsonArrayFlatten(v, newKey)
            else:
                # se v eh string, numero ou booleano
                vlist = [{newKey : v}]

            # combina os jsons de vlist com os que tem em result
            for element in vlist:
                for r in result:
                    e = element.copy() # element eh um dict
                    e.update(r)
                    parcialResult.append(e)
            result = parcialResult
    return result


def jsonArrayFlatten(v, k):
    """
    Recebe uma lista e uma chave
    Devolve uma lista de flat jsons (dict)

    :param v: V
    :param k: K

    """
    if len(v) == 0:
        return [{}]

    result = [] # sera uma lista de dicts
    for element in v:
        if isinstance(element, dict):
            result += jsonFlatten(element, k)
        elif isinstance(element, list):
            # insere * na chave para indicar o nivel dentro do array
            result += jsonArrayFlatten(element, k)
        else:
            result.append({k : element})
    return result


# Module init
if __name__ == "library":
    # JSON hook facility to debug bat JSON data
    json.decoder.errmsg = json_handle_error
