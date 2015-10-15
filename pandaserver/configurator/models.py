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
    datapolicies = Column(String(256))


class PandaSite(Base):
    __tablename__ = 'panda_site'
    __table_args__ = {u'schema': 'atlas_panda'}

    panda_site_name = Column(String(52), primary_key=True)
    site_name = Column(ForeignKey(u'atlas_panda.site.site_name'))
    datapolicies = Column(String(256))


class DdmEndpoint(Base):
    __tablename__ = 'ddm_endpoint'
    __table_args__ = {u'schema': 'atlas_panda'}

    ddm_endpoint_name = Column(String(52), primary_key=True)
    site_name = Column(ForeignKey(u'atlas_panda.site.site_name'))
    ddm_spacetoken_name = Column(String(52))


class PandaDdmRelation(Base):
    __tablename__ = 'panda_ddm_relation'
    __table_args__ = {u'schema': 'atlas_panda'}

    panda_site_name = Column(ForeignKey(u'atlas_panda.panda_site.panda_site_name'), primary_key=True, nullable=False)
    panda_ddm_name = Column(ForeignKey(u'atlas_panda.ddm_endpoint.ddm_endpoint_name'), primary_key=True, nullable=False)
    is_local = Column(String(1))
    is_default = Column(String(1))