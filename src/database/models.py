"""
Database Schema Module for Streamly Recommendation System

This module defines the SQLAlchemy ORM models for the database schema.
"""

from sqlalchemy import (
    Column, Integer, String, Boolean, Float, Date, ForeignKey, Text, Table
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class Account(Base):
    """Account model representing a Streamly account."""

    __tablename__ = 'accounts'

    account_id = Column(Integer, primary_key=True)
    created_at = Column(Date, nullable=False)
    primary_language = Column(String(50), nullable=False)
    profile_count = Column(Integer, nullable=False)
    account_age_days = Column(Integer, nullable=False)

    # Relationships
    profiles = relationship('Profile', back_populates='account', cascade='all, delete-orphan')

    def __repr__(self):
        return f"<Account(id={self.account_id}, profiles={self.profile_count})>"


class Profile(Base):
    """Profile model representing a user profile within an account."""

    __tablename__ = 'profiles'

    profile_id = Column(Integer, primary_key=True)
    account_id = Column(Integer, ForeignKey('accounts.account_id'), nullable=False)
    profile_name = Column(String(100), nullable=False)
    kids_profile = Column(Boolean, nullable=False, default=False)
    age_band = Column(String(20), nullable=False)
    age_band_order = Column(Integer, nullable=False)
    preferred_language = Column(String(50), nullable=False)
    created_at = Column(Date, nullable=False)
    preferences = Column(Text, nullable=True)
    preference_count = Column(Integer, nullable=False, default=0)
    account_age_days = Column(Integer, nullable=False)

    # Relationships
    account = relationship('Account', back_populates='profiles')

    # Indexes
    __table_args__ = (
        {'sqlite_autoincrement': True}
    )

    def __repr__(self):
        return f"<Profile(id={self.profile_id}, name='{self.profile_name}', kids={self.kids_profile})>"


class Title(Base):
    """Title model representing a movie or series in the catalog."""

    __tablename__ = 'titles'

    id = Column(Integer, primary_key=True, autoincrement=True)
    show_id = Column(String(50), unique=True, nullable=False, index=True)
    title_name = Column(String(500), nullable=False, index=True)
    category = Column(String(100), nullable=False, index=True)
    sub_category = Column(String(100), nullable=True, index=True)
    duration = Column(Integer, nullable=False)
    age_rating = Column(String(10), nullable=False, index=True)
    type = Column(String(20), nullable=False, index=True)
    year = Column(Integer, nullable=False, index=True)
    origin_region = Column(String(100), nullable=False, index=True)
    language = Column(String(50), nullable=False, index=True)
    episode_count = Column(Integer, nullable=False, default=1)
    is_kids_content = Column(Boolean, nullable=False, default=False, index=True)
    imdb_rating = Column(Float, nullable=True, index=True)
    imdb_votes = Column(Float, nullable=True)
    has_imdb_data = Column(Boolean, nullable=False, default=False, index=True)
    data_completeness_score = Column(Float, nullable=False, default=0.0)

    # Indexes for common queries
    __table_args__ = (
        {'sqlite_autoincrement': True}
    )

    def __repr__(self):
        return f"<Title(id={self.id}, name='{self.title_name}', type='{self.type}')>"

    def to_dict(self):
        """Convert title to dictionary for API responses."""
        return {
            'show_id': self.show_id,
            'title_name': self.title_name,
            'category': self.category,
            'sub_category': self.sub_category,
            'duration': self.duration,
            'age_rating': self.age_rating,
            'type': self.type,
            'year': self.year,
            'origin_region': self.origin_region,
            'language': self.language,
            'episode_count': self.episode_count,
            'is_kids_content': self.is_kids_content,
            'imdb_rating': self.imdb_rating,
            'imdb_votes': self.imdb_votes
        }
