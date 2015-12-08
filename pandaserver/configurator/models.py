"""
SQLAlchemy models for site hierarchy/relationships generated using sqlacodegen:
$ pip install sqlacodegen
$ sqlacodegen oracle://<user>:<pwd>@<database> --outfile /tmp/models.py --schema atlas_panda
Then take the tables that are relevant for your exercise.
"""
# coding: utf-8
from sqlalchemy import Column, DateTime, ForeignKey, ForeignKeyConstraint, Index, Numeric, String, Table, Text, Unicode, text
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()
metadata = Base.metadata


class Site(Base):
    __tablename__ = 'site'
    __table_args__ = {u'schema': 'atlas_panda'}

    site_name = Column(String(52), primary_key=True)
    role = Column(String(256))
    tier_level = Column(Numeric(1, 0, asdecimal=False))
    state = Column(String(52))

class PandaSite(Base):
    __tablename__ = 'panda_site'
    __table_args__ = {u'schema': 'atlas_panda'}

    panda_site_name = Column(String(52), primary_key=True)
    site_name = Column(ForeignKey(u'atlas_panda.site.site_name', ondelete='CASCADE'))
    storage_site_name = Column(ForeignKey(u'atlas_panda.site.site_name'))
    default_ddm_endpoint = Column(ForeignKey(u'atlas_panda.ddm_endpoint.ddm_endpoint_name'))
    is_local = Column(String(1))

    site = relationship('Site', foreign_keys=site_name)
    storage_site = relationship('Site', foreign_keys=storage_site_name)
    default_endpoint = relationship('DdmEndpoint', foreign_keys=default_ddm_endpoint)

class DdmEndpoint(Base):
    __tablename__ = 'ddm_endpoint'
    __table_args__ = {u'schema': 'atlas_panda'}

    ddm_endpoint_name = Column(String(52), primary_key=True)
    site_name = Column(ForeignKey(u'atlas_panda.site.site_name', ondelete='CASCADE'))
    ddm_spacetoken_name = Column(String(52))
    space_total = Column(Numeric(10, 0, asdecimal=False))
    space_free = Column(Numeric(10, 0, asdecimal=False))
    space_used = Column(Numeric(10, 0, asdecimal=False))
    space_expired = Column(Numeric(10, 0, asdecimal=False))
    space_timestamp = Column(DateTime)
    is_tape = Column(String(1))
    type = Column(String(20))
    blacklisted = Column(String(1))

    site = relationship('Site')


# class PandaDdmRelation(Base):
#     __tablename__ = 'panda_ddm_relation'
#     __table_args__ = {u'schema': 'atlas_panda'}
# 
#     panda_site_name = Column(ForeignKey(u'atlas_panda.panda_site.panda_site_name', ondelete='CASCADE'), primary_key=True, nullable=False)
#     ddm_endpoint_name = Column(ForeignKey(u'atlas_panda.ddm_endpoint.ddm_endpoint_name', ondelete='CASCADE'), primary_key=True, nullable=False)
#     is_local = Column(String(1))
#     is_default = Column(String(1))
#     
#     panda_site = relationship('PandaSite')
#     ddm_endpoint = relationship('DdmEndpoint')


class Schedconfig(Base):
    __tablename__ = 'schedconfig'
    __table_args__ = {u'schema': 'atlas_pandameta'}

    name = Column(String(60), nullable=False)
    nickname = Column(String(60), primary_key=True)
    queue = Column(String(60))
    localqueue = Column(String(50))
    system = Column(String(60), nullable=False)
    sysconfig = Column(String(20))
    environ = Column(String(250))
    gatekeeper = Column(String(120))
    jobmanager = Column(String(80))
    se = Column(String(400))
    ddm = Column(String(120))
    jdladd = Column(String(500))
    globusadd = Column(String(100))
    jdl = Column(String(60))
    jdltxt = Column(String(500))
    version = Column(String(60))
    site = Column(String(60), nullable=False)
    region = Column(String(60))
    gstat = Column(String(60))
    tags = Column(String(200))
    cmd = Column(String(200))
    lastmod = Column(DateTime, nullable=False)
    errinfo = Column(String(80))
    nqueue = Column(Numeric(10, 0, asdecimal=False), nullable=False)
    comment_ = Column(String(500))
    appdir = Column(String(500))
    datadir = Column(String(80))
    tmpdir = Column(String(80))
    wntmpdir = Column(String(80))
    dq2url = Column(String(80))
    special_par = Column(String(80))
    python_path = Column(String(80))
    nodes = Column(Numeric(10, 0, asdecimal=False), nullable=False)
    status = Column(String(10))
    copytool = Column(String(80))
    copysetup = Column(String(200))
    releases = Column(String(500))
    sepath = Column(String(400))
    envsetup = Column(String(200))
    copyprefix = Column(String(500))
    lfcpath = Column(String(80))
    seopt = Column(String(400))
    sein = Column(String(400))
    seinopt = Column(String(400))
    lfchost = Column(String(80))
    cloud = Column(String(60))
    siteid = Column(String(60))
    proxy = Column(String(80))
    retry = Column(String(10))
    queuehours = Column(Numeric(7, 0, asdecimal=False), nullable=False)
    envsetupin = Column(String(200))
    copytoolin = Column(String(180))
    copysetupin = Column(String(200))
    seprodpath = Column(String(400))
    lfcprodpath = Column(String(80))
    copyprefixin = Column(String(360))
    recoverdir = Column(String(80))
    memory = Column(Numeric(10, 0, asdecimal=False), nullable=False)
    maxtime = Column(Numeric(10, 0, asdecimal=False), nullable=False)
    space = Column(Numeric(10, 0, asdecimal=False), nullable=False)
    tspace = Column(DateTime, nullable=False)
    cmtconfig = Column(String(250))
    setokens = Column(String(80))
    glexec = Column(String(10))
    priorityoffset = Column(String(60))
    allowedgroups = Column(String(100))
    defaulttoken = Column(String(100))
    pcache = Column(String(100))
    validatedreleases = Column(String(500))
    accesscontrol = Column(String(20))
    dn = Column(String(100))
    email = Column(String(60))
    allowednode = Column(String(80))
    maxinputsize = Column(Numeric(10, 0, asdecimal=False))
    timefloor = Column(Numeric(5, 0, asdecimal=False))
    depthboost = Column(Numeric(10, 0, asdecimal=False))
    idlepilotsupression = Column(Numeric(10, 0, asdecimal=False))
    pilotlimit = Column(Numeric(10, 0, asdecimal=False))
    transferringlimit = Column(Numeric(10, 0, asdecimal=False))
    cachedse = Column(Numeric(1, 0, asdecimal=False))
    corecount = Column(Numeric(3, 0, asdecimal=False))
    countrygroup = Column(String(64))
    availablecpu = Column(String(64))
    availablestorage = Column(String(64))
    pledgedcpu = Column(String(64))
    pledgedstorage = Column(String(64))
    statusoverride = Column(String(256), server_default=text("'offline'"))
    allowdirectaccess = Column(String(10), server_default=text("'False'"))
    gocname = Column(String(64), server_default=text("'site'"))
    tier = Column(String(15))
    multicloud = Column(String(64))
    lfcregister = Column(String(10))
    stageinretry = Column(Numeric(10, 0, asdecimal=False), server_default=text("2"))
    stageoutretry = Column(Numeric(10, 0, asdecimal=False), server_default=text("2"))
    fairsharepolicy = Column(String(512))
    allowfax = Column(String(64))
    faxredirector = Column(String(256))
    maxwdir = Column(Numeric(10, 0, asdecimal=False))
    celist = Column(String(4000))
    minmemory = Column(Numeric(10, 0, asdecimal=False))
    maxmemory = Column(Numeric(10, 0, asdecimal=False))
    mintime = Column(Numeric(10, 0, asdecimal=False))
    allowjem = Column(String(64))
    catchall = Column(String(512))
    faxdoor = Column(String(128))
    wansourcelimit = Column(Numeric(5, 0, asdecimal=False))
    wansinklimit = Column(Numeric(5, 0, asdecimal=False))
    auto_mcu = Column(Numeric(1, 0, asdecimal=False), server_default=text("0"))
    objectstore = Column(String(512))
    allowhttp = Column(String(64))
    httpredirector = Column(String(256))
    multicloud_append = Column(String(64))
    corepower = Column(Numeric(asdecimal=False))
    wnconnectivity = Column(String(256))
    cloudrshare = Column(String(256))
    sitershare = Column(String(256))
    autosetup_post = Column(String(512))
    autosetup_pre = Column(String(512))
    direct_access_lan = Column(String(32), server_default=text("'False' "))
    direct_access_wan = Column(String(32), server_default=text("'False' "))
