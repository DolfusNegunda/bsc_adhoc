"""
Database Query Utilities for Streamly Recommendation System

This module provides common database queries for the recommendation system.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from sqlalchemy import create_engine, and_, or_, func
from sqlalchemy.orm import sessionmaker

from src.database.models import Account, Profile, Title

# Import config with fallback
try:
    from config.config import DATABASE_PATH
except ImportError:
    DATABASE_PATH = project_root / 'data' / 'streamly.db'


class DatabaseQueries:
    """Provides common database query methods."""

    def __init__(self, db_path=None):
        """
        Initialize DatabaseQueries.

        Args:
            db_path: Path to SQLite database file. Defaults to config setting.
        """
        self.db_path = db_path or DATABASE_PATH
        self.engine = create_engine(f'sqlite:///{self.db_path}', echo=False)
        self.Session = sessionmaker(bind=self.engine)

    def get_session(self):
        """Get a new database session."""
        return self.Session()

    # Account Queries

    def get_account_by_id(self, account_id):
        """Get account by ID."""
        session = self.get_session()
        try:
            return session.query(Account).filter(
                Account.account_id == account_id
            ).first()
        finally:
            session.close()

    def get_all_accounts(self):
        """Get all accounts."""
        session = self.get_session()
        try:
            return session.query(Account).all()
        finally:
            session.close()

    # Profile Queries

    def get_profile_by_id(self, profile_id):
        """Get profile by ID."""
        session = self.get_session()
        try:
            return session.query(Profile).filter(
                Profile.profile_id == profile_id
            ).first()
        finally:
            session.close()

    def get_profiles_by_account(self, account_id):
        """Get all profiles for a given account."""
        session = self.get_session()
        try:
            return session.query(Profile).filter(
                Profile.account_id == account_id
            ).all()
        finally:
            session.close()

    def get_kids_profiles(self):
        """Get all kids profiles."""
        session = self.get_session()
        try:
            return session.query(Profile).filter(
                Profile.kids_profile == True
            ).all()
        finally:
            session.close()

    # Title Queries

    def get_title_by_show_id(self, show_id):
        """Get title by show_id."""
        session = self.get_session()
        try:
            return session.query(Title).filter(
                Title.show_id == show_id
            ).first()
        finally:
            session.close()

    def get_titles_by_age_rating(self, age_rating):
        """Get titles by age rating."""
        session = self.get_session()
        try:
            return session.query(Title).filter(
                Title.age_rating == age_rating
            ).all()
        finally:
            session.close()

    def get_kids_content(self):
        """Get all kids content."""
        session = self.get_session()
        try:
            return session.query(Title).filter(
                Title.is_kids_content == True
            ).all()
        finally:
            session.close()

    def get_titles_by_category(self, category):
        """Get titles by category."""
        session = self.get_session()
        try:
            return session.query(Title).filter(
                Title.category == category
            ).all()
        finally:
            session.close()

    def get_titles_by_language(self, language):
        """Get titles by language."""
        session = self.get_session()
        try:
            return session.query(Title).filter(
                Title.language == language
            ).all()
        finally:
            session.close()

    def get_titles_by_region(self, region):
        """Get titles by origin region."""
        session = self.get_session()
        try:
            return session.query(Title).filter(
                Title.origin_region == region
            ).all()
        finally:
            session.close()

    def get_titles_by_type(self, content_type):
        """Get titles by type (Movie or Series)."""
        session = self.get_session()
        try:
            return session.query(Title).filter(
                Title.type == content_type
            ).all()
        finally:
            session.close()

    def get_top_rated_titles(self, limit=10, min_votes=10):
        """
        Get top rated titles by IMDB rating.

        Args:
            limit: Number of titles to return
            min_votes: Minimum number of IMDB votes required
        """
        session = self.get_session()
        try:
            return session.query(Title).filter(
                and_(
                    Title.has_imdb_data == True,
                    Title.imdb_votes >= min_votes
                )
            ).order_by(Title.imdb_rating.desc()).limit(limit).all()
        finally:
            session.close()

    def get_titles_for_profile(self, profile_id, limit=50):
        """
        Get appropriate titles for a profile based on age and preferences.

        Args:
            profile_id: Profile ID
            limit: Maximum number of titles to return
        """
        session = self.get_session()
        try:
            # Get profile
            profile = session.query(Profile).filter(
                Profile.profile_id == profile_id
            ).first()

            if not profile:
                return []

            # Build query based on profile
            query = session.query(Title)

            # Filter by kids content
            if profile.kids_profile:
                query = query.filter(Title.is_kids_content == True)

            # Filter by age rating
            age_ratings = self._get_appropriate_age_ratings(
                profile.age_band, 
                profile.kids_profile
            )
            query = query.filter(Title.age_rating.in_(age_ratings))

            # Prefer titles with IMDB data
            query = query.order_by(
                Title.has_imdb_data.desc(),
                Title.imdb_rating.desc()
            )

            return query.limit(limit).all()

        finally:
            session.close()

    def _get_appropriate_age_ratings(self, age_band, is_kids):
        """Get appropriate age ratings for a profile."""
        if is_kids:
            if age_band == '<13':
                return ['G', 'PG']
            elif age_band == '13-17':
                return ['G', 'PG', '13+']

        # Adult profiles
        if age_band in ['18-24', '25-34', '35-49', '50+']:
            return ['G', 'PG', '13+', '16+', '18+']

        return ['G', 'PG', '13+']

    def search_titles(self, search_term, limit=20):
        """
        Search titles by name.

        Args:
            search_term: Search term
            limit: Maximum number of results
        """
        session = self.get_session()
        try:
            return session.query(Title).filter(
                Title.title_name.like(f'%{search_term}%')
            ).limit(limit).all()
        finally:
            session.close()

    def get_statistics(self):
        """Get database statistics."""
        session = self.get_session()
        try:
            stats = {
                'total_accounts': session.query(Account).count(),
                'total_profiles': session.query(Profile).count(),
                'total_titles': session.query(Title).count(),
                'kids_profiles': session.query(Profile).filter(
                    Profile.kids_profile == True
                ).count(),
                'kids_content': session.query(Title).filter(
                    Title.is_kids_content == True
                ).count(),
                'movies': session.query(Title).filter(
                    Title.type == 'Movie'
                ).count(),
                'series': session.query(Title).filter(
                    Title.type == 'Series'
                ).count(),
                'titles_with_ratings': session.query(Title).filter(
                    Title.has_imdb_data == True
                ).count(),
                'avg_imdb_rating': session.query(
                    func.avg(Title.imdb_rating)
                ).filter(Title.has_imdb_data == True).scalar(),
                'unique_categories': session.query(Title.category).distinct().count(),
                'unique_languages': session.query(Title.language).distinct().count(),
                'unique_regions': session.query(Title.origin_region).distinct().count()
            }
            return stats
        finally:
            session.close()


def main():
    """Test database queries."""
    print("=" * 80)
    print("TESTING DATABASE QUERIES")
    print("=" * 80)

    db = DatabaseQueries()

    # Test statistics
    print("\n📊 Database Statistics:")
    stats = db.get_statistics()
    for key, value in stats.items():
        if isinstance(value, float):
            print(f"  - {key}: {value:.2f}")
        else:
            print(f"  - {key}: {value}")

    # Test profile query
    print("\n👤 Sample Profile Query:")
    profile = db.get_profile_by_id(1)
    if profile:
        print(f"  Profile: {profile.profile_name}")
        print(f"  Kids: {profile.kids_profile}")
        print(f"  Age Band: {profile.age_band}")
        print(f"  Language: {profile.preferred_language}")

    # Test titles for profile
    print("\n🎬 Sample Titles for Profile:")
    titles = db.get_titles_for_profile(1, limit=5)
    for i, title in enumerate(titles, 1):
        print(f"  {i}. {title.title_name} ({title.year}) - {title.age_rating}")

    # Test top rated
    print("\n⭐ Top Rated Titles:")
    top_titles = db.get_top_rated_titles(limit=5)
    for i, title in enumerate(top_titles, 1):
        print(f"  {i}. {title.title_name} - {title.imdb_rating}/10")

    print("\n✓ All queries executed successfully")


if __name__ == "__main__":
    main()
