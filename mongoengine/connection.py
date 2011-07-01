from pymongo import Connection
import time
import logging
import traceback

__all__ = ['ConnectionError', 'connect']


_connection_defaults = {
    'host': 'localhost',
    'port': 27017,
}
_connection = None
_connection_settings = _connection_defaults.copy()


_db_username = None
_db_password = None
_db = {}


class ConnectionError(Exception):
    pass



def _get_conn():
    global _connection
    if _connection:
        return _connection 
    else:
        while True:
            try:
                _connection = Connection(**_connection_settings)
                return _connection
            except:
                traceback.print_exc()
                logging.info(traceback.format_exc())
                time.sleep(10)
                
def _get_db(dbname):
    return _get_conn()[dbname]
    
def _get_collection(db,collection):
    return db[collection]
    
                

            
    
    
def connect(username=None, password=None, **kwargs):
    """Connect to the database specified by the 'db' argument. Connection 
    settings may be provided here as well if the database is not running on
    the default port on localhost. If authentication is needed, provide
    username and password arguments as well.
    """
    global _connection_settings, _db_username, _db_password
    _connection_settings = dict(_connection_defaults, **kwargs)
    _db_username = username
    _db_password = password
    return _get_conn()

