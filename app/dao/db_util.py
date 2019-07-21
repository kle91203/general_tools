


#Put things in here that will cause other 'heavy' modules to get loaded -- put them in here and not in one of the other util modules to keep the others light.
import os

from app.dao import db


def make_connection_superuser():
    db.conn(user=os.getenv('DB_SUPERUSER_ACCOUNT_NAME'), pw=os.getenv('DB_SUPERUSER_PW'), timeout=30 * 1000)

def make_connection_cross_account():
    db.conn(user=os.getenv('DB_CROSS_ACCOUNT_WRITER_NAME'), pw=os.getenv('DB_CROSS_ACCOUNT_WRITER_PW'))

#todo rename this to make_connection_wrench()
def make_connection_standard():
    db.conn(user=os.getenv('DB_STANDARD_ACCOUNT_NAME'), pw=os.getenv('DB_STANDARD_ACCOUNT_PW'))

def make_connection_read_only():
    db.conn(user=os.getenv('DB_READ_ONLY_NAME'), pw=os.getenv('DB_READ_ONLY_PW'))

def make_connection_client_0002():
    db.conn(user=os.getenv('DB_CLIENT_0002_NAME'), pw=os.getenv('DB_CLIENT_0002_PW'))

def make_connection_client_0001():
    db.conn(user=os.getenv('DB_CLIENT_0001_NAME'), pw=os.getenv('DB_CLIENT_0001_PW'))

def get_role_superuser():
    return os.getenv('DB_SUPERUSER_ACCOUNT_NAME')

def get_role_cross_account():
    return os.getenv('DB_CROSS_ACCOUNT_WRITER_NAME')

def get_pw_cross_account():
    return os.getenv('DB_CROSS_ACCOUNT_WRITER_PW')

#todo rename this to get_role_wrench()
def get_role_standard():
    return os.getenv('DB_STANDARD_ACCOUNT_NAME')

def get_role_read_only():
    return os.getenv('DB_READ_ONLY_NAME')

def get_role_client_0002():
    return os.getenv('DB_CLIENT_0002_NAME')

def get_role_client_0001():
    return os.getenv('DB_CLIENT_0001_NAME')


