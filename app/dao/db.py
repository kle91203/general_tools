import traceback
import psycopg2
import psycopg2.extensions
import psycopg2.extras
import os
import logging

from app.dao import db_util

_connections = {}


def get_conn_with_role(role_name):
    _conn = _connections[role_name]
    return _conn


def conn(db=os.getenv('PGDATABASE', "not set"), user=os.getenv('PGUSER', "not set"), pw=os.getenv('PGPASSWORD', "not set"), host=os.getenv('PGHOST', "not set"), port=os.getenv('PGPORT', "5432"), timeout=60*1000):
    try:
        if _connections.get(user) is None:
            logging.info('Database connection being made.  Host: ' + str(host))
            if str(host) != 'localhost':
                not_localhost = True
            connection = psycopg2.connect("dbname=" + db + " user=" + user + " password=" + pw + " host=" + host + " port=" + str(port) + " options='-c statement_timeout=" + str(timeout) + "'")
            psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)
            connection.set_session(autocommit=True)
            _connections[user] = connection
            return True
        else:
            return True
    except Exception:
        logging.error('DB connection failed for ' + str(user) + ' on ' + str(host) + '\n' + traceback.format_exc())
        return False


def ____for_test_use_only____execute_raw_query(query, payload):
    cur = get_conn_with_role(db_util.get_role_read_only()).cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(query, payload)
    retval = cur.fetchall()
    return retval


def ____for_test_use_only____execute_raw_inset(query, payload):
    cur = get_conn_with_role(db_util.get_role_cross_account()).cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(query, payload)
    rowcount = cur.rowcount
    return rowcount


def ____for_test_use_only____alter_table(query, payload):
    cur = get_conn_with_role(db_util.get_role_superuser()).cursor(cursor_factory=psycopg2.extras.DictCursor)
    logging.info(cur.mogrify(query, payload).decode('utf-8'))
    cur.execute(query, payload)
    rowcount = cur.rowcount
    logging.info('rowcount: ' + str(rowcount))
    status_msg = cur.statusmessage
    logging.info('status_msg: ' + str(status_msg))
    return rowcount, status_msg
