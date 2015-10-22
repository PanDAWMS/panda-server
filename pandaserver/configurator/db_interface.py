#Standard python libraries
import sys

#Specific python libraries
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy import exc

#PanDA server libraries
from config import panda_config
from pandalogger.PandaLogger import PandaLogger

#Configurator libraries
from models import Site, PandaSite, DdmEndpoint, PandaDdmRelation, Schedconfig

#Read connection parameters
__host = panda_config.dbhost
__user = panda_config.dbuser
__passwd = panda_config.dbpasswd
__dbname = panda_config.dbname

#Instantiate logger
_logger = PandaLogger().getLogger('configurator_dbif')

#Log the SQL produced by SQLAlchemy
__echo = True

#Create the SQLAlchemy engine
try:
    __engine = sqlalchemy.create_engine("oracle://%s:%s@%s"%(__user, __passwd, __host), 
                                         echo=__echo)
except exc.SQLAlchemyError:
    _logger.critical("Could not load the DB engine: %s"%sys.exc_info())
    raise


def get_session():
    return sessionmaker(bind=__engine)()


def db_interaction(method):
    """
    NOTE: THIS FUNCTION IS A REMAINDER FROM PREVIOUS DEVELOPEMENT AND IS NOT CURRENTLY USED, 
    BUT SHOULD BE ADAPTED TO REMOVE THE BOILERPLATE CODE IN ALL FUNCTIONS BELOW
    Decorator to wrap a function with the necessary session handling.
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
            _logger.critical("db_session excepted with error: %s"%sys.exc_info())
            session.rollback()
            raise
    return session_wrapper

#TODO: The performance of all write methods could significantly be improved by writing in bulks.
#The current implementation was the fastest way to get it done with the merge method and avoiding
#issues with duplicate keys  
def write_sites_db(session, sites_list):
    """
    Cache the AGIS site information in the PanDA database
    """
    try:
        _logger.debug("Starting write_sites_db")
        for site in sites_list:
            _logger.debug("Site: {0}".format(site['site_name']))
            session.merge(Site(site_name = site['site_name'], 
                                      role = site['role'],
                                      state = site['state']))
        session.commit()
        _logger.debug("Done with write_sites_db")
    except exc.SQLAlchemyError:
        session.rollback()
        _logger.critical('write_sites_db: Could not persist information --> {0}'.format(sys.exc_info()))


def write_panda_sites_db(session, panda_sites_list):
    """
    Cache the AGIS panda site information in the PanDA database
    """
    try:
        _logger.debug("Starting write_panda_sites_db")
        for panda_site in panda_sites_list:
            session.merge(PandaSite(panda_site_name = panda_site['panda_site_name'], 
                                                 site_name = panda_site['site_name'], 
                                                 state = panda_site['state']))
        session.commit()
        _logger.debug("Done with write_panda_sites_db")
    except exc.SQLAlchemyError:
        session.rollback()
        _logger.critical('write_panda_sites_db: Could not persist information --> {0}'.format(sys.exc_info()))


def write_ddm_endpoints_db(session, ddm_endpoints_list):
    """
    Cache the AGIS ddm endpoints in the PanDA database
    """
    try:
        _logger.debug("Starting write_ddm_endpoints_db")
        for ddm_endpoint in ddm_endpoints_list:
            session.merge(DdmEndpoint(ddm_endpoint_name = ddm_endpoint['ddm_endpoint_name'], 
                                        site_name = ddm_endpoint['site_name'], 
                                        ddm_spacetoken_name = ddm_endpoint['ddm_spacetoken_name'],
                                        state = ddm_endpoint['state']))
        session.commit()
        _logger.debug("Done with write_ddm_endpoints_db")
    except exc.SQLAlchemyError:
        session.rollback()
        _logger.critical('write_ddm_endpoints_db: Could not persist information --> {0}'.format(sys.exc_info()))


def read_panda_ddm_relationships_schedconfig(session):
    """
    Read the PanDA - DDM relationships from schedconfig
    """
    try:
        _logger.debug("Starting read_panda_ddm_relationships_schedconfig")
        schedconfig = session.query(Schedconfig.site, Schedconfig.siteid, Schedconfig.ddm).all()
        relationship_tuples = []
        for entry in schedconfig:
            site = entry.site
            panda_site = entry.siteid
            #Schedconfig stores DDM endpoints as a comma separated string. Strip just in case
            if entry.ddm:
                ddm_endpoints = [ddm_endpoint.strip() for ddm_endpoint in entry.ddm.split(',')]
            #Return the tuples and let the caller mingle it the way he wants
            relationship_tuples.append((site, panda_site, ddm_endpoints))
        _logger.debug("Done with read_panda_ddm_relationships_schedconfig")
        return relationship_tuples
    except exc.SQLAlchemyError:
        session.rollback()
        _logger.critical('read_panda_ddm_relationships_schedconfig excepted --> {0}'.format(sys.exc_info()))
        return []


def write_panda_ddm_relations(session, relationships_list):
    """
    Cache the AGIS ddm endpoints in the PanDA database
    """

    _logger.debug("Starting write_panda_ddm_relations")
    for relationship in relationships_list:
        try:
            session.merge(PandaDdmRelation(panda_site_name = relationship['panda_site_name'],
                                           ddm_endpoint_name = relationship['ddm_endpoint_name'],
                                           is_default = relationship['is_default'],
                                           is_local = relationship['is_local']))
            session.commit()
        except exc.IntegrityError:
            session.rollback()
            _logger.error('write_panda_ddm_relations: Could not persist information for relationship {0}. Exception: {1}'.format((relationship['panda_site_name'],
                                                                                                           relationship['ddm_endpoint_name'],
                                                                                                           relationship['is_default'])
                                                                                                          , sys.exc_info()))
        
    _logger.debug("Done with write_panda_ddm_relations")


def read_configurator_sites(session):
    """
    Read the site names from the configurator tables
    """
    try:
        _logger.debug("Starting read_configurator_sites")
        site_object_list = session.query(Site.site_name).all()
        site_set = (entry.site_name for entry in site_object_list)
        _logger.debug("Done with read_configurator_sites")
        return site_set
    except exc.SQLAlchemyError:
        session.rollback()
        _logger.critical('read_configurator_sites excepted --> {0}'.format(sys.exc_info()))
        return []


def read_schedconfig_sites(session):
    """
    Read the site names from the schedconfig table
    """
    try:
        _logger.debug("Starting read_schedconfig_sites")
        site_object_list = session.query(Schedconfig.site).all()
        site_set = (entry.site for entry in site_object_list)
        _logger.debug("Done with read_schedconfig_sites")
        return site_set
    except exc.SQLAlchemyError:
        session.rollback()
        _logger.critical('read_schedconfig_sites excepted --> {0}'.format(sys.exc_info()))
        return []

