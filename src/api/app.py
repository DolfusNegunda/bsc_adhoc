"""
Flask API for Streamly Recommendation System

This module provides REST API endpoints and serves the frontend.
IMPROVED VERSION: Added advanced filtering, pagination, and combined endpoints.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from flask import Flask, jsonify, request, render_template
from flask_cors import CORS
from datetime import datetime
from sqlalchemy import and_, or_, desc, asc

from src.recommendation.engine import RecommendationEngine
from src.database.queries import DatabaseQueries
from src.database.models import Title

# Initialize Flask app
app = Flask(__name__)
CORS(app)  # Enable CORS for frontend access

# Initialize services
recommendation_engine = RecommendationEngine()
db_queries = DatabaseQueries()


# Error handlers
@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors."""
    return jsonify({
        'error': 'Not Found',
        'message': 'The requested resource was not found',
        'status': 404
    }), 404


@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors."""
    return jsonify({
        'error': 'Internal Server Error',
        'message': 'An internal error occurred',
        'status': 500
    }), 500


# Frontend route
@app.route('/', methods=['GET'])
def index():
    """
    Serve the frontend HTML page.

    Returns:
        Rendered HTML template
    """
    return render_template('index.html')


# Health check endpoint
@app.route('/api/health', methods=['GET'])
def health_check():
    """
    Health check endpoint.

    Returns:
        JSON response with API status
    """
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'version': '1.0.0'
    })


# Statistics endpoint
@app.route('/api/statistics', methods=['GET'])
def get_statistics():
    """
    Get database statistics.

    Returns:
        JSON response with database statistics
    """
    try:
        stats = db_queries.get_statistics()
        return jsonify({
            'status': 'success',
            'data': stats
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


# Profile endpoints
@app.route('/api/profiles/<int:profile_id>', methods=['GET'])
def get_profile(profile_id):
    """
    Get profile by ID.

    Args:
        profile_id: Profile ID

    Returns:
        JSON response with profile data
    """
    try:
        profile = db_queries.get_profile_by_id(profile_id)

        if not profile:
            return jsonify({
                'status': 'error',
                'message': f'Profile {profile_id} not found'
            }), 404

        return jsonify({
            'status': 'success',
            'data': {
                'profile_id': profile.profile_id,
                'account_id': profile.account_id,
                'profile_name': profile.profile_name,
                'kids_profile': profile.kids_profile,
                'age_band': profile.age_band,
                'preferred_language': profile.preferred_language,
                'preferences': profile.preferences
            }
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@app.route('/api/accounts/<int:account_id>/profiles', methods=['GET'])
def get_account_profiles(account_id):
    """
    Get all profiles for an account.

    Args:
        account_id: Account ID

    Returns:
        JSON response with list of profiles
    """
    try:
        profiles = db_queries.get_profiles_by_account(account_id)

        profiles_data = [{
            'profile_id': p.profile_id,
            'profile_name': p.profile_name,
            'kids_profile': p.kids_profile,
            'age_band': p.age_band,
            'preferred_language': p.preferred_language
        } for p in profiles]

        return jsonify({
            'status': 'success',
            'data': profiles_data,
            'count': len(profiles_data)
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@app.route('/api/profiles', methods=['GET'])
def get_all_profiles():
    """
    Get all profiles (for dropdown population).

    Returns:
        JSON response with all profiles
    """
    try:
        session = db_queries.get_session()
        try:
            from src.database.models import Profile
            profiles = session.query(Profile).order_by(Profile.profile_id).all()

            profiles_data = [{
                'profile_id': p.profile_id,
                'profile_name': p.profile_name,
                'kids_profile': p.kids_profile,
                'age_band': p.age_band,
                'account_id': p.account_id
            } for p in profiles]

            return jsonify({
                'status': 'success',
                'data': profiles_data,
                'count': len(profiles_data)
            })
        finally:
            session.close()
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


# Recommendation endpoints
@app.route('/api/recommendations/<int:profile_id>', methods=['GET'])
def get_recommendations(profile_id):
    """
    Get personalized recommendations for a profile.

    Args:
        profile_id: Profile ID

    Query Parameters:
        limit: Number of recommendations (default: 10, max: 50)
        exclude: Comma-separated list of show IDs to exclude

    Returns:
        JSON response with recommendations
    """
    try:
        # Get query parameters
        limit = request.args.get('limit', default=10, type=int)
        limit = min(limit, 50)  # Cap at 50

        exclude_param = request.args.get('exclude', default='', type=str)
        exclude_show_ids = [s.strip() for s in exclude_param.split(',') if s.strip()]

        # Get recommendations
        recommendations = recommendation_engine.get_recommendations(
            profile_id=profile_id,
            limit=limit,
            exclude_show_ids=exclude_show_ids if exclude_show_ids else None
        )

        # Format response
        recs_data = [{
            'show_id': rec['show_id'],
            'title_name': rec['title_name'],
            'category': rec['category'],
            'sub_category': rec.get('sub_category'),
            'year': rec['year'],
            'type': rec['type'],
            'duration': rec['duration'],
            'age_rating': rec['age_rating'],
            'language': rec['language'],
            'imdb_rating': rec['imdb_rating'],
            'score': rec['score']
        } for rec in recommendations]

        return jsonify({
            'status': 'success',
            'profile_id': profile_id,
            'data': recs_data,
            'count': len(recs_data)
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@app.route('/api/recommendations/<int:profile_id>/category/<category>', methods=['GET'])
def get_recommendations_by_category(profile_id, category):
    """
    Get recommendations filtered by category.

    Args:
        profile_id: Profile ID
        category: Category to filter by

    Query Parameters:
        limit: Number of recommendations (default: 10, max: 50)

    Returns:
        JSON response with category-filtered recommendations
    """
    try:
        limit = request.args.get('limit', default=10, type=int)
        limit = min(limit, 50)

        recommendations = recommendation_engine.get_recommendations_by_category(
            profile_id=profile_id,
            category=category,
            limit=limit
        )

        recs_data = [{
            'show_id': rec['show_id'],
            'title_name': rec['title_name'],
            'category': rec['category'],
            'year': rec['year'],
            'type': rec['type'],
            'age_rating': rec['age_rating'],
            'imdb_rating': rec['imdb_rating'],
            'score': rec['score']
        } for rec in recommendations]

        return jsonify({
            'status': 'success',
            'profile_id': profile_id,
            'category': category,
            'data': recs_data,
            'count': len(recs_data)
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


# ============================================================================
# FILTERING ENDPOINT
# ============================================================================
@app.route('/api/titles/filter', methods=['GET'])
def filter_titles():
    """
    Filter titles with multiple criteria (BACKEND FILTERING).

    Query Parameters:
        category: Filter by category
        type: Filter by type (Movie/Series)
        age_rating: Filter by age rating
        year_min: Minimum year
        year_max: Maximum year
        kids_only: Boolean for kids content (true/false)
        min_rating: Minimum IMDB rating
        language: Filter by language
        limit: Number of results (default: 50, max: 100)
        sort_by: Sort field (imdb_rating, year, title_name) - default: imdb_rating
        order: Sort order (asc/desc) - default: desc

    Returns:
        JSON response with filtered titles
    """
    try:
        # Get query parameters
        category = request.args.get('category', type=str)
        content_type = request.args.get('type', type=str)
        age_rating = request.args.get('age_rating', type=str)
        year_min = request.args.get('year_min', type=int)
        year_max = request.args.get('year_max', type=int)
        kids_only = request.args.get('kids_only', default='false', type=str).lower() == 'true'
        min_rating = request.args.get('min_rating', type=float)
        language = request.args.get('language', type=str)
        limit = min(request.args.get('limit', default=50, type=int), 100)
        sort_by = request.args.get('sort_by', default='imdb_rating', type=str)
        order = request.args.get('order', default='desc', type=str)

        # Build query
        session = db_queries.get_session()
        try:
            query = session.query(Title)

            # Apply filters
            if category and category != 'all':
                query = query.filter(Title.category == category)

            if content_type and content_type != 'all':
                if content_type == 'kids':
                    query = query.filter(Title.is_kids_content == True)
                else:
                    query = query.filter(Title.type == content_type)

            if age_rating and age_rating != 'all':
                query = query.filter(Title.age_rating == age_rating)

            if year_min:
                query = query.filter(Title.year >= year_min)

            if year_max:
                query = query.filter(Title.year <= year_max)

            if kids_only:
                query = query.filter(Title.is_kids_content == True)

            if min_rating:
                query = query.filter(Title.imdb_rating >= min_rating)

            if language and language != 'all':
                query = query.filter(Title.language == language)

            # Apply sorting
            valid_sort_fields = ['imdb_rating', 'year', 'title_name', 'duration']
            if sort_by not in valid_sort_fields:
                sort_by = 'imdb_rating'

            sort_column = getattr(Title, sort_by)
            if order == 'asc':
                query = query.order_by(asc(sort_column))
            else:
                query = query.order_by(desc(sort_column))

            # Execute query
            results = query.limit(limit).all()

            # Format response
            results_data = [{
                'show_id': t.show_id,
                'title_name': t.title_name,
                'category': t.category,
                'sub_category': t.sub_category,
                'year': t.year,
                'type': t.type,
                'duration': t.duration,
                'age_rating': t.age_rating,
                'language': t.language,
                'imdb_rating': t.imdb_rating,
                'imdb_votes': t.imdb_votes,
                'is_kids_content': t.is_kids_content
            } for t in results]

            return jsonify({
                'status': 'success',
                'data': results_data,
                'count': len(results_data),
                'filters_applied': {
                    'category': category,
                    'type': content_type,
                    'age_rating': age_rating,
                    'year_range': [year_min, year_max] if year_min or year_max else None,
                    'kids_only': kids_only,
                    'min_rating': min_rating,
                    'language': language,
                    'sort_by': sort_by,
                    'order': order
                }
            })
        finally:
            session.close()

    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


# ============================================================================
# PAGINATED TITLES ENDPOINT
# ============================================================================
@app.route('/api/titles', methods=['GET'])
def get_titles_paginated():
    """
    Get paginated titles.

    Query Parameters:
        page: Page number (default: 1)
        per_page: Items per page (default: 20, max: 100)
        sort_by: Sort field (rating, year, title_name)
        order: asc or desc

    Returns:
        JSON response with paginated titles
    """
    try:
        page = max(request.args.get('page', default=1, type=int), 1)
        per_page = min(request.args.get('per_page', default=20, type=int), 100)
        sort_by = request.args.get('sort_by', default='imdb_rating', type=str)
        order = request.args.get('order', default='desc', type=str)

        session = db_queries.get_session()
        try:
            query = session.query(Title)

            # Apply sorting
            valid_sort_fields = ['imdb_rating', 'year', 'title_name']
            if sort_by not in valid_sort_fields:
                sort_by = 'imdb_rating'

            sort_column = getattr(Title, sort_by)
            if order == 'desc':
                query = query.order_by(desc(sort_column))
            else:
                query = query.order_by(asc(sort_column))

            # Get total count
            total = query.count()

            # Apply pagination
            offset = (page - 1) * per_page
            results = query.offset(offset).limit(per_page).all()

            return jsonify({
                'status': 'success',
                'data': [t.to_dict() for t in results],
                'pagination': {
                    'page': page,
                    'per_page': per_page,
                    'total': total,
                    'pages': (total + per_page - 1) // per_page
                }
            })
        finally:
            session.close()

    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


# ============================================================================
# COMBINED DASHBOARD ENDPOINT (Reduces API calls)
# ============================================================================
@app.route('/api/dashboard/<int:profile_id>', methods=['GET'])
def get_dashboard_data(profile_id):
    """
    Get all data needed for dashboard in one call.

    Args:
        profile_id: Profile ID

    Returns:
        JSON with profile info, recommendations, statistics, and categories
    """
    try:
        # Get profile
        profile = db_queries.get_profile_by_id(profile_id)
        if not profile:
            return jsonify({
                'status': 'error',
                'message': 'Profile not found'
            }), 404

        # Get recommendations
        recommendations = recommendation_engine.get_recommendations(profile_id, limit=10)

        # Get categories
        session = db_queries.get_session()
        try:
            categories = session.query(Title.category).distinct().all()
            categories_list = sorted([c[0] for c in categories if c[0] and c[0] != 'Unknown'])
        finally:
            session.close()

        # Get stats
        stats = db_queries.get_statistics()

        return jsonify({
            'status': 'success',
            'data': {
                'profile': {
                    'profile_id': profile.profile_id,
                    'profile_name': profile.profile_name,
                    'kids_profile': profile.kids_profile,
                    'age_band': profile.age_band,
                    'preferred_language': profile.preferred_language,
                    'preferences': profile.preferences
                },
                'recommendations': recommendations,
                'categories': categories_list,
                'statistics': stats
            }
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


# Title endpoints
@app.route('/api/titles/<show_id>', methods=['GET'])
def get_title(show_id):
    """
    Get title details by show ID.

    Args:
        show_id: Show ID

    Returns:
        JSON response with title details
    """
    try:
        title = db_queries.get_title_by_show_id(show_id)

        if not title:
            return jsonify({
                'status': 'error',
                'message': f'Title {show_id} not found'
            }), 404

        return jsonify({
            'status': 'success',
            'data': title.to_dict()
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@app.route('/api/titles/<show_id>/similar', methods=['GET'])
def get_similar_titles(show_id):
    """
    Get titles similar to a given title.

    Args:
        show_id: Show ID

    Query Parameters:
        limit: Number of similar titles (default: 10, max: 20)

    Returns:
        JSON response with similar titles
    """
    try:
        limit = request.args.get('limit', default=10, type=int)
        limit = min(limit, 20)

        similar = recommendation_engine.get_similar_titles(
            show_id=show_id,
            limit=limit
        )

        similar_data = [{
            'show_id': s['show_id'],
            'title_name': s['title_name'],
            'category': s['category'],
            'year': s['year'],
            'imdb_rating': s['imdb_rating'],
            'similarity_score': s['score']
        } for s in similar]

        return jsonify({
            'status': 'success',
            'show_id': show_id,
            'data': similar_data,
            'count': len(similar_data)
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@app.route('/api/titles/search', methods=['GET'])
def search_titles():
    """
    Search titles by name.

    Query Parameters:
        q: Search query
        limit: Number of results (default: 20, max: 50)

    Returns:
        JSON response with search results
    """
    try:
        query = request.args.get('q', default='', type=str)
        limit = request.args.get('limit', default=20, type=int)
        limit = min(limit, 50)

        if not query:
            return jsonify({
                'status': 'error',
                'message': 'Query parameter "q" is required'
            }), 400

        results = db_queries.search_titles(query, limit=limit)

        results_data = [{
            'show_id': t.show_id,
            'title_name': t.title_name,
            'category': t.category,
            'year': t.year,
            'type': t.type,
            'age_rating': t.age_rating,
            'imdb_rating': t.imdb_rating
        } for t in results]

        return jsonify({
            'status': 'success',
            'query': query,
            'data': results_data,
            'count': len(results_data)
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


# Categories endpoint
@app.route('/api/categories', methods=['GET'])
def get_categories():
    """
    Get all unique categories.

    Returns:
        JSON response with list of categories
    """
    try:
        session = db_queries.get_session()
        try:
            categories = session.query(Title.category).distinct().all()
            categories_list = sorted([c[0] for c in categories if c[0] and c[0] != 'Unknown'])

            return jsonify({
                'status': 'success',
                'data': categories_list,
                'count': len(categories_list)
            })
        finally:
            session.close()
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


if __name__ == '__main__':
    print("=" * 80)
    print("🎬 STREAMLY RECOMMENDATION SYSTEM")
    print("=" * 80)
    print("\nStarting server...")
    print("\n📍 Frontend: http://localhost:5000")
    print("📍 API: http://localhost:5000/api")
    print("\nPress CTRL+C to stop the server")
    print("=" * 80)
    app.run(debug=True, host='0.0.0.0', port=5000)
