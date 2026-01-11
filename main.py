"""
Streamly Recommendation System - Main Workflow
==============================================

This script runs the complete data pipeline:
1. Data cleaning and preprocessing
2. Database setup and data loading
3. Flask web application startup

Usage:
    python main.py [--skip-cleaning] [--skip-db] [--port PORT]

Options:
    --skip-cleaning    Skip data cleaning step (use existing cleaned data)
    --skip-db          Skip database setup (use existing database)
    --drop-db          Drop existing database tables before creating new ones
    --port PORT        Port for Flask app (default: 5000)
    --help             Show this help message
"""

import sys
import os
import argparse
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root))


def print_banner(text):
    """Print a formatted banner."""
    print("\n" + "=" * 80)
    print(f"  {text}")
    print("=" * 80 + "\n")


def print_step(step_num, total_steps, description):
    """Print step information."""
    print(f"\n[STEP {step_num}/{total_steps}] {description}")
    print("-" * 80)


def run_data_cleaning():
    """Run data cleaning pipeline."""
    print_step(1, 3, "DATA CLEANING & PREPROCESSING")

    try:
        from src.data_processing.data_cleaner import DataCleaner

        cleaner = DataCleaner()
        success = cleaner.run()

        if success:
            print("\n✅ Data cleaning completed successfully!")
            return True
        else:
            print("\n❌ Data cleaning failed!")
            return False

    except Exception as e:
        print(f"\n❌ Error during data cleaning: {e}")
        import traceback
        traceback.print_exc()
        return False


def run_database_setup(drop_existing=False):
    """Run database setup and data loading."""
    print_step(2, 3, "DATABASE SETUP & DATA LOADING")

    try:
        from src.database.db_setup import DatabaseManager

        db_manager = DatabaseManager()
        success = db_manager.run(drop_existing=drop_existing)

        if success:
            print("\n✅ Database setup completed successfully!")
            return True
        else:
            print("\n❌ Database setup failed!")
            return False

    except Exception as e:
        print(f"\n❌ Error during database setup: {e}")
        import traceback
        traceback.print_exc()
        return False


def run_flask_app(port=5000):
    """Run Flask web application."""
    print_step(3, 3, "STARTING WEB APPLICATION")

    try:
        from src.api.app import app

        print(f"\n🎬 Starting Streamly Recommendation System...")
        print(f"\n📍 Frontend: http://localhost:{port}")
        print(f"📍 API: http://localhost:{port}/api")
        print(f"\nPress CTRL+C to stop the server")
        print("=" * 80 + "\n")

        app.run(debug=True, host='0.0.0.0', port=port)

    except KeyboardInterrupt:
        print("\n\n🛑 Server stopped by user")
    except Exception as e:
        print(f"\n❌ Error starting Flask app: {e}")
        import traceback
        traceback.print_exc()
        return False


def check_data_files():
    """Check if required data files exist."""
    required_files = [
        project_root / 'data' / 'raw' / 'titles.csv',
        project_root / 'data' / 'raw' / 'profiles.csv'
    ]

    missing_files = [f for f in required_files if not f.exists()]

    if missing_files:
        print("\n⚠️  WARNING: Missing required data files:")
        for f in missing_files:
            print(f"  - {f}")
        print("\nPlease ensure CSV files are in the data/raw/ directory.")
        return False

    return True


def main():
    """Main workflow execution."""
    # Check if this is a Flask reloader restart
    if os.environ.get('WERKZEUG_RUN_MAIN') == 'true':
        # This is the reloader process, skip all setup and just run the app
        run_flask_app(port=5000)
        return
    
    parser = argparse.ArgumentParser(
        description='Streamly Recommendation System - Complete Workflow',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        '--skip-cleaning',
        action='store_true',
        help='Skip data cleaning step (use existing cleaned data)'
    )

    parser.add_argument(
        '--skip-db',
        action='store_true',
        help='Skip database setup (use existing database)'
    )

    parser.add_argument(
        '--drop-db',
        action='store_true',
        help='Drop existing database tables before creating new ones'
    )

    parser.add_argument(
        '--port',
        type=int,
        default=5000,
        help='Port for Flask application (default: 5000)'
    )

    args = parser.parse_args()

    # Print welcome banner
    print_banner("🎬 STREAMLY RECOMMENDATION SYSTEM")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Auto-detect if database exists and skip setup automatically
    db_path = project_root / 'data' / 'streamly.db'
    if db_path.exists() and not args.drop_db:
        print("\n✅ Database already exists - skipping setup steps")
        args.skip_cleaning = True
        args.skip_db = True

    # Check for required data files
    if not args.skip_cleaning:
        print("\nChecking for required data files...")
        if not check_data_files():
            print("\n❌ Cannot proceed without required data files.")
            sys.exit(1)
        print("✅ All required data files found!")

    # Step 1: Data Cleaning
    if not args.skip_cleaning:
        if not run_data_cleaning():
            print("\n❌ Workflow failed at data cleaning step.")
            sys.exit(1)
    else:
        print("\n⏭️  Skipping data cleaning (using existing cleaned data)")

    # Step 2: Database Setup
    if not args.skip_db:
        if not run_database_setup(drop_existing=args.drop_db):
            print("\n❌ Workflow failed at database setup step.")
            sys.exit(1)
    else:
        print("\n⏭️  Skipping database setup (using existing database)")

    # Step 3: Run Flask App
    print_banner("✅ SETUP COMPLETE - STARTING APPLICATION")
    run_flask_app(port=args.port)


if __name__ == "__main__":
    main()