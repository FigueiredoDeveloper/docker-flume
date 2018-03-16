# -*- coding: utf-8 -*-

import cx_Oracle


def oracle_connect(user, passwd, conn_str):
    """
    Connect to a Oracle DB

    :param user: Connection user login name
    :param passwd: Connection user password
    :param conn_str: Connection string
    :return: cx_Oracle.cursor
    """

    try:
        con = cx_Oracle.connect("%s/%s@%s" % (user, passwd, conn_str))
        return con

    except Exception as e:
        raise e
