"""db_interface is the interface to the DB using the SQLAlchemy library. It is a module (and not a class)
as this is the easiest way to implement a Singleton pattern in python. 
"""

import sys

import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy import exc

from config import panda_config

#Read connection parameters
__host = panda_config.dbhost
__user = panda_config.dbuser
__passwd = panda_config.dbpasswd
__dbname = panda_config.dbname

#Log the SQL produced by SQLAlchemy
__echo = True

#Create the SQLAlchemy engine
try:
    __engine = sqlalchemy.create_engine("oracle://%s:%s@%s"%(__user, __passwd, __host), 
                                         echo=__echo)
except exc.SQLAlchemyError:
    __logger.critical("Could not load the DB engine: %s"%sys.exc_info())
    raise


def db_session(method):
    """Decorator to wrap a function with the necessary session handling.
    FIXME: Getting a session has a 50ms overhead. See if there are better ways.
    FIXME: Note that this method inserts the session as the first parameter of the function. There might
        be more elegant solutions to do this.
    """
    def session_wrapper(*args, **kwargs):
        try:
            session = sessionmaker(bind=__engine)()
            result = method(session, *args, **kwargs)
            session.commit()
            return result
        except exc.SQLAlchemyError:
            __logger.critical("db_session excepted with error: %s"%sys.exc_info())
            session.rollback()
            raise
    return session_wrapper


def get_session():
    return sessionmaker(bind=__engine)()