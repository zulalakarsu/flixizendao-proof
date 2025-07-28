"""SQLAlchemy database models for storing Spotify contribution data"""
import datetime

from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, Float, DateTime, UniqueConstraint, ARRAY
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Contributors(Base):
    """
    Tracks contributor information and hashed PII for privacy.
    """
    __tablename__ = 'contributors'

    id = Column(Integer, primary_key=True)
    wallet_address = Column(String, nullable=False)
    ip_address_hash = Column(String)
    storage_source = Column(String, nullable=False, default="google-drive")
    storage_user_id_hash = Column(String)
    created_at = Column(DateTime, default=datetime.datetime.now(datetime.UTC))


class Contributions(Base):
    """
    Tracks file contributions made by contributors.
    """
    __tablename__ = 'contributions'

    id = Column(Integer, primary_key=True)
    score = Column(Float, default=0.0, nullable=False)
    quality = Column(Float, default=0.0, nullable=False)
    uniqueness = Column(Float, default=0.0, nullable=False)
    authenticity = Column(Float, default=0.0, nullable=False)
    ownership = Column(Float, default=0.0, nullable=False)
    valid = Column(Boolean, default=False, nullable=False)
    file_id = Column(Integer, nullable=False)
    coordinates = Column(Integer)
    unique_coordinates = Column(Integer)
    errors = Column(ARRAY(String))
    contributor_id = Column(Integer, ForeignKey('contributors.id'), nullable=False)
    created_at = Column(DateTime, default=datetime.datetime.now(datetime.UTC))


class Coordinates(Base):
    """
    Stores all coordinates anonymously to track unique contributions.
    """
    __tablename__ = 'coordinates'

    id = Column(Integer, primary_key=True)
    longitude = Column(Float, nullable=False)
    latitude = Column(Float, nullable=False)
    contributor_id = Column(Integer, ForeignKey('contributors.id'), nullable=False)
    created_at = Column(DateTime, default=datetime.datetime.now(datetime.UTC))

    __table_args__ = (
        UniqueConstraint('latitude', 'longitude', name='uix_lat_lng'),
    )