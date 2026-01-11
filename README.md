# 🎬 Streamly Recommendation System

A personalized content recommendation system for a video streaming platform, built with Python, Flask, and SQLite.

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Project Structure](#project-structure)
- [Setup Instructions](#setup-instructions)
- [Usage](#usage)
- [API Documentation](#api-documentation)
- [Architecture](#architecture)
- [Technologies Used](#technologies-used)

---

## 🎯 Overview

Streamly is a recommendation system designed to provide personalized content suggestions for streaming platform users. The system analyzes user profiles (age, preferences, language) and matches them with appropriate content using a multi-factor scoring algorithm.

**Key Highlights:**
- Content-based recommendation engine
- Age-appropriate filtering
- Multi-factor scoring (preferences, language, ratings, recency)
- RESTful API with Flask
- Interactive web interface
- SQLite database for efficient data storage

---

## ✨ Features

### 🔍 Search & Filter
- Search titles by name
- Filter by category, type, age rating, and year
- Advanced filtering with multiple criteria

### 🎯 Personalized Recommendations
- Profile-based recommendations
- Category-specific suggestions
- Configurable recommendation count
- Score-based ranking

### 👤 Profile Management
- Multiple profiles per account
- Kids profile support
- Age-appropriate content filtering
- Language preferences

### 📊 Analytics
- Database statistics dashboard
- Content distribution metrics
- Rating analytics

---

## 📁 Project Structure

```
streamly-recommendation-system/
│
├── main.py                      # Main workflow orchestrator
├── requirements.txt             # Python dependencies
├── README.md                    # This file
│
├── data/
│   ├── raw/                     # Raw CSV files (titles.csv, profiles.csv)
│   ├── cleaned/                 # Cleaned data files
│   └── streamly.db              # SQLite database (generated)
│
├── src/
│   ├── data/
│   │   └── data_cleaner.py      # Data cleaning and preprocessing
│   │
│   ├── database/
│   │   ├── models.py            # SQLAlchemy ORM models
│   │   ├── db_setup.py          # Database creation and loading
│   │   └── queries.py           # Database query utilities
│   │
│   ├── recommendation/
│   │   └── engine.py            # Recommendation algorithm
│   │
│   └── api/
│       ├── app.py               # Flask API and routes
│       └── templates/
│           └── index.html       # Web interface
│
├── config/
│   └── config.py                # Configuration settings
│
└── logs/                        # Application logs (generated)
```

---

## 🚀 Setup Instructions

### Prerequisites

- Python 3.8 or higher
- Git

### Step 1: Clone the Repository

```bash
git clone <repository-url>
cd <repository-name>
```

### Step 2: Create Virtual Environment (Using Python 11)

```bash
python -m venv .venv
```

### Step 3: Activate Virtual Environment

**Windows:**
```bash
.venv\Scripts\activate
```

**macOS/Linux:**
```bash
source .venv/bin/activate
```

### Step 4: Install Dependencies

```bash
uv pip install -r requirements.txt
```

*Note: If you don't have `uv` installed, you can use regular pip:*
```bash
pip install -r requirements.txt
```

### Step 5: Add Data Files

Place your CSV files in the `data/raw/` directory:
- `titles.csv` - Content catalog
- `profiles.csv` - User profiles

### Step 6: Run the Application

```bash
python main.py
```

This will:
1. Clean and preprocess the data
2. Create and populate the SQLite database
3. Start the Flask web server

The application will be available at: **http://localhost:5000**

---

## 💻 Usage

### Running the Complete Workflow

```bash
# Run everything (cleaning, database setup, and app)
python main.py

# Skip data cleaning (use existing cleaned data)
python main.py --skip-cleaning

# Skip database setup (use existing database)
python main.py --skip-db

# Run on a custom port
python main.py --port 8080

# Drop and recreate database
python main.py --drop-db
```

### Running Individual Components

**Data Cleaning Only:**
```bash
python -m src.data.data_cleaner
```

**Database Setup Only:**
```bash
python -m src.database.db_setup
```

**Flask App Only:**
```bash
python -m src.api.app
```

---

## 📡 API Documentation

### Base URL
```
http://localhost:5000/api
```

### Endpoints

#### **GET /api/statistics**
Get database statistics.

**Response:**
```json
{
  "status": "success",
  "data": {
    "total_titles": 1000,
    "movies": 799,
    "series": 201,
    "avg_imdb_rating": 6.35
  }
}
```

#### **GET /api/profiles**
Get all profiles.

**Response:**
```json
{
  "status": "success",
  "data": [
    {
      "profile_id": 1,
      "profile_name": "Gran",
      "display_name": "1. Gran",
      "kids_profile": false,
      "age_band": "35-49"
    }
  ]
}
```

#### **GET /api/recommendations/{profile_id}**
Get personalized recommendations for a profile.

**Parameters:**
- `limit` (optional): Number of recommendations (default: 10, max: 50)
- `exclude` (optional): Comma-separated list of show IDs to exclude

**Response:**
```json
{
  "status": "success",
  "profile_id": 1,
  "data": [
    {
      "show_id": "tt1234567",
      "title_name": "Example Movie",
      "category": "Drama",
      "year": 2020,
      "score": 85.5
    }
  ]
}
```

#### **GET /api/titles/filter**
Filter titles with multiple criteria.

**Parameters:**
- `category`: Filter by category
- `type`: Filter by type (Movie/Series)
- `age_rating`: Filter by age rating
- `year_min`: Minimum year
- `year_max`: Maximum year
- `limit`: Number of results (default: 50, max: 100)

**Response:**
```json
{
  "status": "success",
  "data": [...],
  "count": 25,
  "filters_applied": {
    "category": "Drama",
    "type": "Movie"
  }
}
```

#### **GET /api/titles/search**
Search titles by name.

**Parameters:**
- `q`: Search query (required)
- `limit`: Number of results (default: 20, max: 50)

**Response:**
```json
{
  "status": "success",
  "query": "inception",
  "data": [...],
  "count": 5
}
```

---

## 🏗️ Architecture

### Data Flow

```
Raw CSV Files
    ↓
Data Cleaning (data_cleaner.py)
    ↓
Cleaned CSV Files
    ↓
Database Setup (db_setup.py)
    ↓
SQLite Database
    ↓
Recommendation Engine (engine.py)
    ↓
Flask API (app.py)
    ↓
Web Interface (index.html)
```

### Recommendation Algorithm

The system uses a **content-based filtering** approach with multi-factor scoring:

1. **Preference Match (30%)**: Matches user preferences with content categories
2. **Language Match (20%)**: Prioritizes content in user's preferred language
3. **IMDB Rating (25%)**: Considers content quality
4. **Recency (10%)**: Favors newer content
5. **Popularity (15%)**: Based on IMDB vote count

**Age-Appropriate Filtering:**
- Mandatory filtering based on profile age band
- Kids profiles only see G/PG content
- Adult profiles see all appropriate content

---

## 🛠️ Technologies Used

### Backend
- **Python 3.8+**: Core programming language
- **Flask**: Web framework and REST API
- **SQLAlchemy**: ORM for database operations
- **SQLite**: Lightweight database
- **Pandas**: Data manipulation and cleaning

### Frontend
- **HTML5/CSS3**: Structure and styling
- **JavaScript (Vanilla)**: Client-side logic
- **Fetch API**: Backend communication

### Development Tools
- **uv**: Fast Python package installer
- **Git**: Version control

---

## 📊 Data Schema

### Tables

**accounts**
- account_id (PK)
- created_at
- primary_language
- profile_count
- account_age_days

**profiles**
- profile_id (PK)
- account_id (FK)
- profile_name
- kids_profile
- age_band
- preferred_language
- preferences
- created_at

**titles**
- id (PK)
- show_id (Unique)
- title_name
- category
- sub_category
- duration
- age_rating
- type
- year
- origin_region
- language
- episode_count
- is_kids_content
- imdb_rating
- imdb_votes

---

## 🔧 Configuration

Edit `config/config.py` to customize:
- Database path
- Data directories
- Age rating mappings
- Logging settings

---

## 📝 Logging

Logs are automatically generated in the `logs/` directory:
- `cleaning_report_YYYYMMDD_HHMMSS.txt`: Data cleaning logs
- `database_setup_YYYYMMDD_HHMMSS.txt`: Database setup logs

---

## 🐛 Troubleshooting

### Issue: "Module not found" errors
**Solution:** Ensure virtual environment is activated and dependencies are installed:
```bash
source .venv/bin/activate  # or .venv\Scripts\activate on Windows
uv pip install -r requirements.txt
```

### Issue: "No such file or directory: data/raw/titles.csv"
**Solution:** Place your CSV files in the `data/raw/` directory.

### Issue: Database errors
**Solution:** Delete the database and recreate:
```bash
rm data/streamly.db
python main.py --drop-db
```

### Issue: Port already in use
**Solution:** Use a different port:
```bash
python main.py --port 8080
```

---

## 📈 Future Enhancements

- [ ] Collaborative filtering
- [ ] User viewing history tracking
- [ ] A/B testing framework
- [ ] Recommendation diversity optimization
- [ ] Real-time recommendation updates
- [ ] User feedback integration
- [ ] Advanced analytics dashboard

---

## 👥 Contributors

- Dolfus Negunda

---

## 📄 License

This project is part of a case study for Streamly.

---

## 🙏 Acknowledgments

- Case study provided by Streamly
- IMDB for rating data
- Open-source community for tools and libraries

---

**For questions or issues, please contact: dnegunda@bscglobal.com**
