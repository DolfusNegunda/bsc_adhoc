"""
Data Cleaning Module for Streamly Recommendation System

This module handles loading, analyzing, and cleaning the raw data files.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
from datetime import datetime

# Import config with fallback
try:
    from config.config import (
        RAW_DATA_DIR,
        CLEANED_DATA_DIR,
        AGE_BAND_ORDER,
        KIDS_AGE_RATINGS,
        ADULT_AGE_RATINGS,
        LOG_DIR
    )
except ImportError:
    # Fallback if config import fails
    RAW_DATA_DIR = project_root / 'data' / 'raw'
    CLEANED_DATA_DIR = project_root / 'data' / 'cleaned'
    LOG_DIR = project_root / 'logs'
    AGE_BAND_ORDER = {
        '<13': 1,
        '13-17': 2,
        '18-24': 3,
        '25-34': 4,
        '35-49': 5,
        '50+': 6
    }
    KIDS_AGE_RATINGS = ['G', 'PG']
    ADULT_AGE_RATINGS = ['13+', '16+', '18+']


class DataCleaner:
    """Handles data loading, validation, and cleaning operations."""

    def __init__(self):
        """Initialize the DataCleaner."""
        self.titles_df = None
        self.profiles_df = None
        self.accounts_df = None
        self.cleaning_log = []

    def load_data(self):
        """Load raw CSV files."""
        print("=" * 80)
        print("LOADING RAW DATA")
        print("=" * 80)

        try:
            # Load titles
            titles_path = RAW_DATA_DIR / 'titles.csv'
            self.titles_df = pd.read_csv(titles_path)
            print(f"✓ Loaded titles.csv: {self.titles_df.shape[0]} rows, {self.titles_df.shape[1]} columns")

            # Load profiles
            profiles_path = RAW_DATA_DIR / 'profiles.csv'
            self.profiles_df = pd.read_csv(profiles_path)
            print(f"✓ Loaded profiles.csv: {self.profiles_df.shape[0]} rows, {self.profiles_df.shape[1]} columns")

            print("\n✓ Data loading complete")
            return True

        except FileNotFoundError as e:
            print(f"✗ Error: {e}")
            print(f"\nPlease ensure CSV files are in: {RAW_DATA_DIR}")
            return False

    def analyze_data_quality(self):
        """Analyze data quality and identify anomalies."""
        print("\n" + "=" * 80)
        print("DATA QUALITY ANALYSIS")
        print("=" * 80)

        # Titles analysis
        print("\n📊 TITLES DATASET")
        print("-" * 80)
        print(f"Total records: {len(self.titles_df)}")
        print(f"Duplicate show_ids: {self.titles_df['show_id'].duplicated().sum()}")
        print(f"Missing values: {self.titles_df.isnull().sum().sum()}")
        print(f"\nMissing by column:")
        missing_titles = self.titles_df.isnull().sum()
        for col, count in missing_titles[missing_titles > 0].items():
            print(f"  - {col}: {count} ({count/len(self.titles_df)*100:.1f}%)")

        # Identify anomalies
        print("\n⚠️  ANOMALIES DETECTED:")

        # Kids content with adult ratings
        kids_adult_rating = self.titles_df[
            (self.titles_df['is_kids_content'] == True) & 
            (self.titles_df['age_rating'].isin(['18+', '16+', '13+']))
        ]
        if len(kids_adult_rating) > 0:
            print(f"  - Kids content with adult ratings: {len(kids_adult_rating)}")

        # Movies with multiple episodes
        movies_multi_ep = self.titles_df[
            (self.titles_df['type'] == 'Movie') & 
            (self.titles_df['episode_count'] > 1)
        ]
        if len(movies_multi_ep) > 0:
            print(f"  - Movies with >1 episode: {len(movies_multi_ep)}")

        # Profiles analysis
        print("\n👥 PROFILES DATASET")
        print("-" * 80)
        print(f"Total records: {len(self.profiles_df)}")
        print(f"Duplicate profile_ids: {self.profiles_df['profile_id'].duplicated().sum()}")
        print(f"Missing values: {self.profiles_df.isnull().sum().sum()}")
        print(f"Unique accounts: {self.profiles_df['account_id'].nunique()}")

    def clean_titles(self):
        """Clean titles dataset."""
        print("\n" + "=" * 80)
        print("CLEANING TITLES DATASET")
        print("=" * 80)

        # Create a copy
        titles_clean = self.titles_df.copy()

        # 1. Fix kids_content inconsistency
        print("\n1. Fixing kids_content inconsistencies...")
        inconsistent_count = len(titles_clean[
            (titles_clean['is_kids_content'] == True) & 
            (titles_clean['age_rating'].isin(['18+', '16+']))
        ])

        titles_clean.loc[
            (titles_clean['is_kids_content'] == True) & 
            (titles_clean['age_rating'].isin(['18+', '16+'])),
            'is_kids_content'
        ] = False

        print(f"   ✓ Fixed {inconsistent_count} kids content with adult ratings")
        self.cleaning_log.append(f"Fixed {inconsistent_count} kids content with 18+/16+ ratings")

        # 2. Fix episode count for movies
        print("\n2. Fixing episode count for movies...")
        movies_count = len(titles_clean[
            (titles_clean['type'] == 'Movie') & 
            (titles_clean['episode_count'] > 1)
        ])

        titles_clean.loc[
            (titles_clean['type'] == 'Movie') & 
            (titles_clean['episode_count'] > 1),
            'episode_count'
        ] = 1

        print(f"   ✓ Fixed {movies_count} movies with incorrect episode counts")
        self.cleaning_log.append(f"Fixed {movies_count} movies with >1 episode")

        # 3. Handle missing categories
        print("\n3. Handling missing categories...")
        missing_cat = titles_clean['category'].isnull().sum()
        titles_clean['category'] = titles_clean['category'].fillna('Unknown')
        print(f"   ✓ Filled {missing_cat} missing categories")
        self.cleaning_log.append(f"Filled {missing_cat} missing categories")

        # 4. Handle missing sub_categories
        print("\n4. Handling missing sub-categories...")
        missing_subcat = titles_clean['sub_category'].isnull().sum()
        titles_clean['sub_category'] = titles_clean.apply(
            lambda row: row['category'] if pd.isnull(row['sub_category']) else row['sub_category'],
            axis=1
        )
        print(f"   ✓ Filled {missing_subcat} missing sub-categories")
        self.cleaning_log.append(f"Filled {missing_subcat} missing sub-categories")

        # 5. Handle missing origin_region
        print("\n5. Handling missing origin regions...")
        missing_region = titles_clean['origin_region'].isnull().sum()
        titles_clean['origin_region'] = titles_clean['origin_region'].fillna('Unknown')
        print(f"   ✓ Filled {missing_region} missing origin regions")
        self.cleaning_log.append(f"Filled {missing_region} missing origin regions")

        # 6. Handle missing language
        print("\n6. Handling missing languages...")
        missing_lang = titles_clean['language'].isnull().sum()
        titles_clean['language'] = titles_clean['language'].fillna('Unknown')
        print(f"   ✓ Filled {missing_lang} missing languages")
        self.cleaning_log.append(f"Filled {missing_lang} missing languages")

        # 7. Add quality flags
        print("\n7. Adding data quality flags...")
        titles_clean['has_imdb_data'] = titles_clean['imdb_rating'].notnull()
        titles_clean['data_completeness_score'] = (
            titles_clean['category'].notnull().astype(int) +
            titles_clean['sub_category'].notnull().astype(int) +
            titles_clean['origin_region'].notnull().astype(int) +
            titles_clean['language'].notnull().astype(int) +
            titles_clean['imdb_rating'].notnull().astype(int)
        ) / 5
        print("   ✓ Added has_imdb_data and data_completeness_score")
        self.cleaning_log.append("Added data quality flags")

        self.titles_df = titles_clean
        print("\n✓ Titles cleaning complete")

    def clean_profiles(self):
        """Clean profiles dataset."""
        print("\n" + "=" * 80)
        print("CLEANING PROFILES DATASET")
        print("=" * 80)

        profiles_clean = self.profiles_df.copy()

        # 1. Parse preferences
        print("\n1. Processing preferences...")
        profiles_clean['preferences_list'] = profiles_clean['preferences'].str.split(', ')
        profiles_clean['preference_count'] = profiles_clean['preferences_list'].apply(len)
        print("   ✓ Added preferences_list and preference_count")
        self.cleaning_log.append("Processed preferences into list format")

        # 2. Add age band ordering
        print("\n2. Adding age band ordering...")
        profiles_clean['age_band_order'] = profiles_clean['age_band'].map(AGE_BAND_ORDER)
        print("   ✓ Added age_band_order")
        self.cleaning_log.append("Added age_band_order for sorting")

        # 3. Parse dates
        print("\n3. Parsing dates...")
        profiles_clean['created_at_date'] = pd.to_datetime(profiles_clean['created_at'])
        profiles_clean['account_age_days'] = (
            pd.Timestamp.now() - profiles_clean['created_at_date']
        ).dt.days
        print("   ✓ Added created_at_date and account_age_days")
        self.cleaning_log.append("Parsed dates and calculated account age")

        self.profiles_df = profiles_clean
        print("\n✓ Profiles cleaning complete")

    def create_accounts_dataset(self):
        """Create accounts dataset from profiles."""
        print("\n" + "=" * 80)
        print("CREATING ACCOUNTS DATASET")
        print("=" * 80)

        # Aggregate by account_id
        accounts = self.profiles_df.groupby('account_id').agg({
            'created_at': 'min',
            'profile_id': 'count'
        }).reset_index()
        accounts.columns = ['account_id', 'created_at', 'profile_count']

        # Get primary language (most common per account)
        account_languages = self.profiles_df.groupby('account_id')['preferred_language'].agg(
            lambda x: x.mode()[0] if len(x.mode()) > 0 else x.iloc[0]
        ).reset_index()
        account_languages.columns = ['account_id', 'primary_language']

        # Merge
        accounts = accounts.merge(account_languages, on='account_id')

        # Parse dates
        accounts['created_at_date'] = pd.to_datetime(accounts['created_at'])
        accounts['account_age_days'] = (
            pd.Timestamp.now() - accounts['created_at_date']
        ).dt.days

        self.accounts_df = accounts
        print(f"\n✓ Created accounts dataset: {len(accounts)} accounts")
        self.cleaning_log.append(f"Created accounts dataset from profiles ({len(accounts)} accounts)")

    def save_cleaned_data(self):
        """Save cleaned datasets to CSV files."""
        print("\n" + "=" * 80)
        print("SAVING CLEANED DATA")
        print("=" * 80)

        # Ensure directory exists
        CLEANED_DATA_DIR.mkdir(parents=True, exist_ok=True)

        # Save titles
        titles_path = CLEANED_DATA_DIR / 'titles_cleaned.csv'
        self.titles_df.to_csv(titles_path, index=False)
        print(f"✓ Saved: {titles_path}")

        # Save profiles
        profiles_path = CLEANED_DATA_DIR / 'profiles_cleaned.csv'
        self.profiles_df.to_csv(profiles_path, index=False)
        print(f"✓ Saved: {profiles_path}")

        # Save accounts
        accounts_path = CLEANED_DATA_DIR / 'accounts_cleaned.csv'
        self.accounts_df.to_csv(accounts_path, index=False)
        print(f"✓ Saved: {accounts_path}")

        print("\n✓ All cleaned data saved successfully")

    def generate_cleaning_report(self):
        """Generate and save cleaning report."""
        print("\n" + "=" * 80)
        print("GENERATING CLEANING REPORT")
        print("=" * 80)

        report = {
            'timestamp': datetime.now().isoformat(),
            'titles_records': len(self.titles_df),
            'profiles_records': len(self.profiles_df),
            'accounts_records': len(self.accounts_df),
            'cleaning_steps': self.cleaning_log
        }

        # Save report
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        report_path = LOG_DIR / f'cleaning_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt'

        with open(report_path, 'w') as f:
            f.write("STREAMLY DATA CLEANING REPORT\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Timestamp: {report['timestamp']}\n\n")
            f.write(f"Records Processed:\n")
            f.write(f"  - Titles: {report['titles_records']}\n")
            f.write(f"  - Profiles: {report['profiles_records']}\n")
            f.write(f"  - Accounts: {report['accounts_records']}\n\n")
            f.write("Cleaning Steps:\n")
            for i, step in enumerate(report['cleaning_steps'], 1):
                f.write(f"  {i}. {step}\n")

        print(f"✓ Report saved: {report_path}")

    def run(self):
        """Execute the complete data cleaning pipeline."""
        print("\n" + "=" * 80)
        print("STREAMLY DATA CLEANING PIPELINE")
        print("=" * 80)
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # Load data
        if not self.load_data():
            return False

        # Analyze
        self.analyze_data_quality()

        # Clean
        self.clean_titles()
        self.clean_profiles()
        self.create_accounts_dataset()

        # Save
        self.save_cleaned_data()
        self.generate_cleaning_report()

        print("\n" + "=" * 80)
        print("✅ DATA CLEANING PIPELINE COMPLETE")
        print("=" * 80)
        print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        return True


def main():
    """Main entry point."""
    cleaner = DataCleaner()
    success = cleaner.run()

    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
