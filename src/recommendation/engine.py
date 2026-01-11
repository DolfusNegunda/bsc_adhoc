"""
Recommendation Engine for Streamly

This module implements a content-based recommendation algorithm that considers:
- User profile (age, preferences, language)
- Content metadata (category, language, region, ratings)
- Age-appropriate filtering
- Preference matching
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from typing import List, Dict, Optional
import random

from src.database.queries import DatabaseQueries
from src.database.models import Profile, Title


class RecommendationEngine:
    """
    Content-based recommendation engine for Streamly.

    Scoring factors:
    - Age appropriateness (mandatory filter)
    - Category/preference match (high weight)
    - Language match (medium weight)
    - IMDB rating (medium weight)
    - Recency (low weight)
    """

    def __init__(self, db_path=None):
        """
        Initialize the recommendation engine.

        Args:
            db_path: Optional path to database file
        """
        self.db = DatabaseQueries(db_path)

        # Scoring weights
        self.weights = {
            'preference_match': 3.0,
            'language_match': 2.0,
            'imdb_rating': 2.5,
            'recency': 1.0,
            'popularity': 1.5
        }

    def get_recommendations(
        self, 
        profile_id: int, 
        limit: int = 10,
        exclude_show_ids: Optional[List[str]] = None
    ) -> List[Dict]:
        """
        Get personalized recommendations for a profile.

        Args:
            profile_id: Profile ID to get recommendations for
            limit: Number of recommendations to return
            exclude_show_ids: List of show IDs to exclude (already watched)

        Returns:
            List of recommended titles with scores
        """
        # Get profile
        profile = self.db.get_profile_by_id(profile_id)
        if not profile:
            return []

        # Get candidate titles (age-appropriate)
        candidates = self._get_candidate_titles(profile)

        # Exclude already watched
        if exclude_show_ids:
            candidates = [t for t in candidates if t.show_id not in exclude_show_ids]

        # Score each candidate
        scored_titles = []
        for title in candidates:
            score = self._calculate_score(title, profile)
            scored_titles.append({
                'title': title,
                'score': score,
                'show_id': title.show_id,
                'title_name': title.title_name,
                'category': title.category,
                'year': title.year,
                'imdb_rating': title.imdb_rating,
                'age_rating': title.age_rating,
                'type': title.type,
                'language': title.language,
                'duration': title.duration
            })

        # Sort by score (descending)
        scored_titles.sort(key=lambda x: x['score'], reverse=True)

        # Return top N
        return scored_titles[:limit]

    def _get_candidate_titles(self, profile: Profile) -> List[Title]:
        """
        Get age-appropriate candidate titles for a profile.

        Args:
            profile: User profile

        Returns:
            List of candidate titles
        """
        # Get appropriate age ratings
        age_ratings = self._get_age_ratings(profile)

        # Get all titles from database
        session = self.db.get_session()
        try:
            query = session.query(Title)

            # Filter by kids content
            if profile.kids_profile:
                query = query.filter(Title.is_kids_content == True)

            # Filter by age rating
            query = query.filter(Title.age_rating.in_(age_ratings))

            return query.all()
        finally:
            session.close()

    def _get_age_ratings(self, profile: Profile) -> List[str]:
        """Get appropriate age ratings for a profile."""
        if profile.kids_profile:
            if profile.age_band == '<13':
                return ['G', 'PG']
            elif profile.age_band == '13-17':
                return ['G', 'PG', '13+']

        # Adult profiles
        if profile.age_band in ['18-24', '25-34', '35-49', '50+']:
            return ['G', 'PG', '13+', '16+', '18+']

        return ['G', 'PG', '13+']

    def _calculate_score(self, title: Title, profile: Profile) -> float:
        """
        Calculate recommendation score for a title.

        Args:
            title: Title to score
            profile: User profile

        Returns:
            Recommendation score (0-100)
        """
        score = 0.0

        # 1. Preference match (0-10 points)
        preference_score = self._score_preference_match(title, profile)
        score += preference_score * self.weights['preference_match']

        # 2. Language match (0-10 points)
        language_score = self._score_language_match(title, profile)
        score += language_score * self.weights['language_match']

        # 3. IMDB rating (0-10 points)
        rating_score = self._score_imdb_rating(title)
        score += rating_score * self.weights['imdb_rating']

        # 4. Recency (0-10 points)
        recency_score = self._score_recency(title)
        score += recency_score * self.weights['recency']

        # 5. Popularity (0-10 points)
        popularity_score = self._score_popularity(title)
        score += popularity_score * self.weights['popularity']

        return round(score, 2)

    def _score_preference_match(self, title: Title, profile: Profile) -> float:
        """
        Score based on category/preference match.

        Returns:
            Score 0-10
        """
        if not profile.preferences:
            return 5.0  # Neutral score if no preferences

        # Parse preferences
        preferences = [p.strip() for p in profile.preferences.split(',')]

        # Check if title category matches any preference
        if title.category in preferences:
            return 10.0

        # Check sub-category
        if title.sub_category and title.sub_category in preferences:
            return 8.0

        # Partial match (case-insensitive)
        title_cat_lower = title.category.lower()
        for pref in preferences:
            if pref.lower() in title_cat_lower or title_cat_lower in pref.lower():
                return 6.0

        return 3.0  # No match

    def _score_language_match(self, title: Title, profile: Profile) -> float:
        """
        Score based on language match.

        Returns:
            Score 0-10
        """
        if title.language == 'Unknown':
            return 5.0  # Neutral for unknown

        # Exact match
        if title.language.lower() == profile.preferred_language.lower():
            return 10.0

        # English is widely understood
        if title.language.lower() == 'english' or title.language == 'en':
            return 7.0

        return 3.0  # Different language

    def _score_imdb_rating(self, title: Title) -> float:
        """
        Score based on IMDB rating.

        Returns:
            Score 0-10
        """
        if not title.has_imdb_data or title.imdb_rating is None:
            return 5.0  # Neutral for no rating

        # Normalize IMDB rating (1-10) to score (0-10)
        # IMDB ratings typically range from 1-10
        return title.imdb_rating

    def _score_recency(self, title: Title) -> float:
        """
        Score based on release year (prefer recent content).

        Returns:
            Score 0-10
        """
        current_year = 2026  # As per case study context

        # Calculate age of content
        age = current_year - title.year

        # Score based on age
        if age <= 1:
            return 10.0  # Very recent
        elif age <= 3:
            return 8.0   # Recent
        elif age <= 5:
            return 6.0   # Fairly recent
        elif age <= 10:
            return 4.0   # Older
        else:
            return 2.0   # Classic/old

    def _score_popularity(self, title: Title) -> float:
        """
        Score based on IMDB votes (popularity indicator).

        Returns:
            Score 0-10
        """
        if not title.has_imdb_data or title.imdb_votes is None:
            return 5.0  # Neutral for no data

        # Logarithmic scale for votes
        # 10 votes = 0, 100 votes = 3.3, 1000 votes = 6.6, 10000+ votes = 10
        import math

        if title.imdb_votes < 10:
            return 0.0

        score = (math.log10(title.imdb_votes) - 1) * 3.33
        return min(10.0, max(0.0, score))

    def get_recommendations_by_category(
        self,
        profile_id: int,
        category: str,
        limit: int = 10
    ) -> List[Dict]:
        """
        Get recommendations filtered by category.

        Args:
            profile_id: Profile ID
            category: Category to filter by
            limit: Number of recommendations

        Returns:
            List of recommended titles
        """
        # Get all recommendations
        all_recs = self.get_recommendations(profile_id, limit=100)

        # Filter by category
        filtered = [r for r in all_recs if r['category'] == category]

        return filtered[:limit]

    def get_similar_titles(
        self,
        show_id: str,
        limit: int = 10
    ) -> List[Dict]:
        """
        Get titles similar to a given title.

        Args:
            show_id: Show ID to find similar titles for
            limit: Number of similar titles to return

        Returns:
            List of similar titles
        """
        # Get the reference title
        title = self.db.get_title_by_show_id(show_id)
        if not title:
            return []

        # Get all titles in same category
        session = self.db.get_session()
        try:
            candidates = session.query(Title).filter(
                Title.category == title.category,
                Title.show_id != show_id  # Exclude the reference title
            ).all()

            # Score based on similarity
            scored = []
            for candidate in candidates:
                similarity_score = self._calculate_similarity(title, candidate)
                scored.append({
                    'title': candidate,
                    'score': similarity_score,
                    'show_id': candidate.show_id,
                    'title_name': candidate.title_name,
                    'category': candidate.category,
                    'year': candidate.year,
                    'imdb_rating': candidate.imdb_rating
                })

            # Sort by similarity
            scored.sort(key=lambda x: x['score'], reverse=True)

            return scored[:limit]
        finally:
            session.close()

    def _calculate_similarity(self, title1: Title, title2: Title) -> float:
        """Calculate similarity score between two titles."""
        score = 0.0

        # Same category (already filtered)
        score += 10.0

        # Same sub-category
        if title1.sub_category == title2.sub_category:
            score += 10.0

        # Same type (Movie/Series)
        if title1.type == title2.type:
            score += 5.0

        # Similar year (within 5 years)
        year_diff = abs(title1.year - title2.year)
        if year_diff <= 5:
            score += (5 - year_diff) * 2

        # Similar rating
        if title1.imdb_rating and title2.imdb_rating:
            rating_diff = abs(title1.imdb_rating - title2.imdb_rating)
            if rating_diff <= 1.0:
                score += (1.0 - rating_diff) * 10

        return score


def main():
    """Test the recommendation engine."""
    print("=" * 80)
    print("TESTING RECOMMENDATION ENGINE")
    print("=" * 80)

    engine = RecommendationEngine()

    # Test for profile 1
    print("\n📊 Getting recommendations for Profile 1...")
    recommendations = engine.get_recommendations(profile_id=1, limit=10)

    print(f"\n✓ Found {len(recommendations)} recommendations\n")

    for i, rec in enumerate(recommendations, 1):
        print(f"{i}. {rec['title_name']} ({rec['year']})")
        print(f"   Category: {rec['category']} | Rating: {rec['age_rating']}")
        print(f"   IMDB: {rec['imdb_rating']}/10 | Score: {rec['score']}")
        print()

    # Test similar titles
    if recommendations:
        first_show_id = recommendations[0]['show_id']
        print(f"\n🔍 Finding similar titles to: {recommendations[0]['title_name']}")
        similar = engine.get_similar_titles(first_show_id, limit=5)

        print(f"\n✓ Found {len(similar)} similar titles\n")
        for i, sim in enumerate(similar, 1):
            print(f"{i}. {sim['title_name']} ({sim['year']})")
            print(f"   Similarity Score: {sim['score']}")
            print()

    print("=" * 80)
    print("✅ RECOMMENDATION ENGINE TEST COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()
