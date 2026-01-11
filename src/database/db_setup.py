"""
Database Setup Module for Streamly Recommendation System

This module handles database creation, connection, and data loading.
"""

import sys
from pathlib import Path

# Add project root to path - more robust approach
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

# Now import after path is set
import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# Import models
from src.database.models import Base, Account, Profile, Title

# Import config with fallback
try:
    from config.config import DATABASE_PATH, CLEANED_FILES, LOG_DIR
except ImportError:
    # Fallback if config import fails
    DATABASE_PATH = project_root / 'data' / 'streamly.db'
    CLEANED_FILES = {
        'accounts': project_root / 'data' / 'cleaned' / 'accounts_cleaned.csv',
        'profiles': project_root / 'data' / 'cleaned' / 'profiles_cleaned.csv',
        'titles': project_root / 'data' / 'cleaned' / 'titles_cleaned.csv',
    }
    LOG_DIR = project_root / 'logs'


class DatabaseManager:
    """Manages database operations including creation and data loading."""

    def __init__(self, db_path=None):
        """
        Initialize the DatabaseManager.

        Args:
            db_path: Path to SQLite database file. Defaults to config setting.
        """
        self.db_path = db_path or DATABASE_PATH
        self.engine = None
        self.Session = None

    def create_engine_and_session(self):
        """Create database engine and session factory."""
        print(f"\nConnecting to database: {self.db_path}")

        # Ensure directory exists
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

        # Create engine
        self.engine = create_engine(
            f'sqlite:///{self.db_path}',
            echo=False  # Set to True for SQL debugging
        )

        # Create session factory
        self.Session = sessionmaker(bind=self.engine)

        print("✓ Database engine created")

    def create_tables(self, drop_existing=False):
        """
        Create database tables.

        Args:
            drop_existing: If True, drop existing tables before creating new ones.
        """
        print("\n" + "=" * 80)
        print("CREATING DATABASE TABLES")
        print("=" * 80)

        if drop_existing:
            print("\n⚠️  Dropping existing tables...")
            Base.metadata.drop_all(self.engine)
            print("✓ Existing tables dropped")

        # Create all tables
        Base.metadata.create_all(self.engine)

        # Verify tables were created
        inspector = inspect(self.engine)
        tables = inspector.get_table_names()

        print(f"\n✓ Created {len(tables)} tables:")
        for table in tables:
            columns = inspector.get_columns(table)
            indexes = inspector.get_indexes(table)
            print(f"  - {table}: {len(columns)} columns, {len(indexes)} indexes")

    def load_accounts(self, session):
        """Load accounts data from CSV."""
        print("\n1. Loading accounts...")

        # Read CSV
        accounts_df = pd.read_csv(CLEANED_FILES['accounts'])

        # Convert to Account objects
        accounts = []
        for _, row in accounts_df.iterrows():
            account = Account(
                account_id=int(row['account_id']),
                created_at=pd.to_datetime(row['created_at']).date(),
                primary_language=str(row['primary_language']),
                profile_count=int(row['profile_count']),
                account_age_days=int(row['account_age_days'])
            )
            accounts.append(account)

        # Bulk insert
        session.bulk_save_objects(accounts)
        session.commit()

        print(f"   ✓ Loaded {len(accounts)} accounts")
        return len(accounts)

    def load_profiles(self, session):
        """Load profiles data from CSV."""
        print("\n2. Loading profiles...")

        # Read CSV
        profiles_df = pd.read_csv(CLEANED_FILES['profiles'])

        # Convert to Profile objects
        profiles = []
        for _, row in profiles_df.iterrows():
            profile = Profile(
                profile_id=int(row['profile_id']),
                account_id=int(row['account_id']),
                profile_name=str(row['profile_name']),
                kids_profile=bool(row['kids_profile']),
                age_band=str(row['age_band']),
                age_band_order=int(row['age_band_order']),
                preferred_language=str(row['preferred_language']),
                created_at=pd.to_datetime(row['created_at']).date(),
                preferences=str(row['preferences']) if pd.notna(row['preferences']) else '',
                preference_count=int(row['preference_count']),
                account_age_days=int(row['account_age_days'])
            )
            profiles.append(profile)

        # Bulk insert
        session.bulk_save_objects(profiles)
        session.commit()

        print(f"   ✓ Loaded {len(profiles)} profiles")
        return len(profiles)

    def load_titles(self, session):
        """Load titles data from CSV."""
        print("\n3. Loading titles...")

        # Read CSV
        titles_df = pd.read_csv(CLEANED_FILES['titles'])

        # Convert to Title objects
        titles = []
        for _, row in titles_df.iterrows():
            title = Title(
                show_id=str(row['show_id']),
                title_name=str(row['title_name']),
                category=str(row['category']),
                sub_category=str(row['sub_category']) if pd.notna(row['sub_category']) else None,
                duration=int(row['duration']),
                age_rating=str(row['age_rating']),
                type=str(row['type']),
                year=int(row['year']),
                origin_region=str(row['origin_region']),
                language=str(row['language']),
                episode_count=int(row['episode_count']),
                is_kids_content=bool(row['is_kids_content']),
                imdb_rating=float(row['imdb_rating']) if pd.notna(row['imdb_rating']) else None,
                imdb_votes=float(row['imdb_votes']) if pd.notna(row['imdb_votes']) else None,
                has_imdb_data=bool(row['has_imdb_data']),
                data_completeness_score=float(row['data_completeness_score'])
            )
            titles.append(title)

        # Bulk insert
        session.bulk_save_objects(titles)
        session.commit()

        print(f"   ✓ Loaded {len(titles)} titles")
        return len(titles)

    def load_all_data(self):
        """Load all data from CSV files into database."""
        print("\n" + "=" * 80)
        print("LOADING DATA INTO DATABASE")
        print("=" * 80)

        session = self.Session()

        try:
            # Load data in order (accounts first due to foreign keys)
            accounts_count = self.load_accounts(session)
            profiles_count = self.load_profiles(session)
            titles_count = self.load_titles(session)

            print("\n" + "=" * 80)
            print("✓ DATA LOADING COMPLETE")
            print("=" * 80)
            print(f"\nTotal records loaded:")
            print(f"  - Accounts: {accounts_count}")
            print(f"  - Profiles: {profiles_count}")
            print(f"  - Titles: {titles_count}")

            return {
                'accounts': accounts_count,
                'profiles': profiles_count,
                'titles': titles_count
            }

        except Exception as e:
            session.rollback()
            print(f"\n✗ Error loading data: {e}")
            raise
        finally:
            session.close()

    def verify_data(self):
        """Verify data was loaded correctly."""
        print("\n" + "=" * 80)
        print("VERIFYING DATABASE")
        print("=" * 80)

        session = self.Session()

        try:
            # Count records
            accounts_count = session.query(Account).count()
            profiles_count = session.query(Profile).count()
            titles_count = session.query(Title).count()

            print(f"\n✓ Database verification:")
            print(f"  - Accounts: {accounts_count}")
            print(f"  - Profiles: {profiles_count}")
            print(f"  - Titles: {titles_count}")

            # Test some queries
            print("\n✓ Testing sample queries:")

            # Query 1: Get all profiles for account 1
            account_profiles = session.query(Profile).filter(
                Profile.account_id == 1
            ).all()
            print(f"  - Profiles for account 1: {len(account_profiles)}")

            # Query 2: Get kids content
            kids_titles = session.query(Title).filter(
                Title.is_kids_content == True
            ).count()
            print(f"  - Kids content titles: {kids_titles}")

            # Query 3: Get titles by age rating
            pg_titles = session.query(Title).filter(
                Title.age_rating == 'PG'
            ).count()
            print(f"  - PG rated titles: {pg_titles}")

            # Query 4: Get titles by language
            english_titles = session.query(Title).filter(
                Title.language == 'en'
            ).count()
            print(f"  - English titles: {english_titles}")

            # Query 5: Get titles with IMDB ratings
            rated_titles = session.query(Title).filter(
                Title.has_imdb_data == True
            ).count()
            print(f"  - Titles with IMDB ratings: {rated_titles}")

            print("\n✓ All queries executed successfully")

        except Exception as e:
            print(f"\n✗ Verification error: {e}")
            raise
        finally:
            session.close()

    def generate_report(self, load_stats):
        """Generate database setup report."""
        print("\n" + "=" * 80)
        print("GENERATING SETUP REPORT")
        print("=" * 80)

        report = {
            'timestamp': datetime.now().isoformat(),
            'database_path': str(self.db_path),
            'records_loaded': load_stats
        }

        # Save report
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        report_path = LOG_DIR / f'database_setup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt'

        with open(report_path, 'w') as f:
            f.write("STREAMLY DATABASE SETUP REPORT\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Timestamp: {report['timestamp']}\n")
            f.write(f"Database: {report['database_path']}\n\n")
            f.write("Records Loaded:\n")
            for table, count in report['records_loaded'].items():
                f.write(f"  - {table.capitalize()}: {count}\n")

        print(f"\n✓ Report saved: {report_path}")

    def run(self, drop_existing=False):
        """Execute the complete database setup pipeline."""
        print("\n" + "=" * 80)
        print("STREAMLY DATABASE SETUP PIPELINE")
        print("=" * 80)
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        try:
            # Create engine and session
            self.create_engine_and_session()

            # Create tables
            self.create_tables(drop_existing=drop_existing)

            # Load data
            load_stats = self.load_all_data()

            # Verify
            self.verify_data()

            # Generate report
            self.generate_report(load_stats)

            print("\n" + "=" * 80)
            print("✅ DATABASE SETUP COMPLETE")
            print("=" * 80)
            print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"\nDatabase location: {self.db_path}")

            return True

        except Exception as e:
            print(f"\n✗ Database setup failed: {e}")
            import traceback
            traceback.print_exc()
            return False


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description='Setup Streamly database')
    parser.add_argument(
        '--drop',
        action='store_true',
        help='Drop existing tables before creating new ones'
    )

    args = parser.parse_args()

    # Create database manager
    db_manager = DatabaseManager()

    # Run setup
    success = db_manager.run(drop_existing=args.drop)

    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
