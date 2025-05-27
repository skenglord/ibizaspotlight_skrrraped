"""
Quality Scoring Engine for Event Data
Implements field-specific validation and confidence scoring
"""

import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


class QualityScorer:
    """Calculate quality scores for event data fields"""
    
    def __init__(self):
        """Initialize quality scorer with validation rules"""
        self.field_weights = {
            "title": 0.25,
            "location": 0.20,
            "dateTime": 0.25,
            "lineUp": 0.15,
            "ticketInfo": 0.15
        }
        
    def calculate_event_quality(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate quality scores for an entire event
        
        Args:
            event_data: Event data dictionary
            
        Returns:
            Dictionary with quality scores and metadata
        """
        scores = {}
        validation_details = {}
        
        # Calculate individual field scores
        scores["title"], validation_details["title"] = self._score_title(
            event_data.get("title", "")
        )
        
        scores["location"], validation_details["location"] = self._score_location(
            event_data.get("location", {})
        )
        
        scores["dateTime"], validation_details["dateTime"] = self._score_datetime(
            event_data.get("dateTime", {})
        )
        
        scores["lineUp"], validation_details["lineUp"] = self._score_lineup(
            event_data.get("lineUp", [])
        )
        
        scores["ticketInfo"], validation_details["ticketInfo"] = self._score_ticket_info(
            event_data.get("ticketInfo", {})
        )
        
        # Calculate overall score
        overall_score = self._calculate_overall_score(scores)
        
        return {
            "_quality": {
                "scores": scores,
                "overall": overall_score,
                "lastCalculated": datetime.utcnow()
            },
            "_validation": validation_details
        }
    
    def _score_title(self, title: str) -> Tuple[float, Dict]:
        """Score title field"""
        score = 0.0
        flags = []
        
        if not title:
            return 0.0, {
                "confidence": 0.0,
                "flags": ["missing_title"],
                "lastChecked": datetime.utcnow()
            }
        
        # Length check
        if len(title) >= 5:
            score += 0.3
        else:
            flags.append("title_too_short")
        
        # Contains date pattern
        if re.search(r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}', title):
            score += 0.2
        
        # Contains venue/artist name
        if len(title.split()) >= 2:
            score += 0.2
        
        # No excessive special characters
        special_char_ratio = len(re.findall(r'[^a-zA-Z0-9\s\-&]', title)) / len(title)
        if special_char_ratio < 0.2:
            score += 0.2
        else:
            flags.append("excessive_special_chars")
        
        # Proper capitalization
        if title[0].isupper() and not title.isupper():
            score += 0.1
        
        return min(score, 1.0), {
            "confidence": min(score, 1.0),
            "flags": flags,
            "lastChecked": datetime.utcnow()
        }
    
    def _score_location(self, location: Dict) -> Tuple[float, Dict]:
        """Score location field"""
        score = 0.0
        flags = []
        
        if not location:
            return 0.0, {
                "confidence": 0.0,
                "flags": ["missing_location"],
                "lastChecked": datetime.utcnow()
            }
        
        # Venue name
        if location.get("venue"):
            score += 0.3
            # Known Ibiza venues get bonus
            known_venues = ["Hï Ibiza", "Ushuaïa", "Pacha", "Amnesia", "DC10", "Privilege"]
            if any(venue in location["venue"] for venue in known_venues):
                score += 0.1
        else:
            flags.append("missing_venue")
        
        # Address
        if location.get("address"):
            score += 0.2
        else:
            flags.append("missing_address")
        
        # City (should be Ibiza)
        if location.get("city"):
            score += 0.2
            if "ibiza" in location["city"].lower():
                score += 0.1
        else:
            flags.append("missing_city")
        
        # Coordinates
        if location.get("coordinates"):
            coords = location["coordinates"]
            if coords.get("lat") and coords.get("lng"):
                # Check if coordinates are in Ibiza area
                if 38.8 <= coords["lat"] <= 39.1 and 1.2 <= coords["lng"] <= 1.6:
                    score += 0.2
                else:
                    flags.append("coordinates_outside_ibiza")
        
        return min(score, 1.0), {
            "confidence": min(score, 1.0),
            "flags": flags,
            "lastChecked": datetime.utcnow()
        }
    
    def _score_datetime(self, datetime_info: Dict) -> Tuple[float, Dict]:
        """Score datetime field"""
        score = 0.0
        flags = []
        
        if not datetime_info:
            return 0.0, {
                "confidence": 0.0,
                "flags": ["missing_datetime"],
                "lastChecked": datetime.utcnow()
            }
        
        # Start date
        if datetime_info.get("start"):
            score += 0.4
            # Check if date is reasonable (not too far in past or future)
            start_date = datetime_info["start"]
            if isinstance(start_date, str):
                try:
                    start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
                except:
                    flags.append("invalid_date_format")
            
            if isinstance(start_date, datetime):
                now = datetime.utcnow()
                if start_date < now - timedelta(days=30):
                    flags.append("date_too_far_past")
                elif start_date > now + timedelta(days=365):
                    flags.append("date_too_far_future")
                else:
                    score += 0.1
        else:
            flags.append("missing_start_date")
        
        # End date (optional but good to have)
        if datetime_info.get("end"):
            score += 0.2
        
        # Display text
        if datetime_info.get("displayText"):
            score += 0.2
        
        # Timezone
        if datetime_info.get("timezone"):
            score += 0.1
            if datetime_info["timezone"] in ["Europe/Madrid", "CET", "CEST"]:
                score += 0.05
        
        return min(score, 1.0), {
            "confidence": min(score, 1.0),
            "flags": flags,
            "lastChecked": datetime.utcnow()
        }
    
    def _score_lineup(self, lineup: List[Dict]) -> Tuple[float, Dict]:
        """Score lineup field"""
        score = 0.0
        flags = []
        item_validation = {}
        
        if not lineup:
            return 0.0, {
                "confidence": 0.0,
                "flags": ["missing_lineup"],
                "lastChecked": datetime.utcnow(),
                "itemValidation": {}
            }
        
        # Has at least one artist
        if len(lineup) > 0:
            score += 0.4
        
        # Score individual artists
        valid_artists = 0
        for artist in lineup:
            artist_score = 0.0
            artist_flags = []
            
            if artist.get("name"):
                artist_score += 0.6
                # Check name quality
                if len(artist["name"]) >= 2:
                    artist_score += 0.2
                else:
                    artist_flags.append("name_too_short")
                
                # Has headliner designation
                if "headliner" in artist:
                    artist_score += 0.1
                
                # Has genre
                if artist.get("genre"):
                    artist_score += 0.1
                
                item_validation[artist["name"]] = {
                    "confidence": min(artist_score, 1.0),
                    "verified": artist_score >= 0.8
                }
                
                if artist_score >= 0.6:
                    valid_artists += 1
            else:
                artist_flags.append("missing_artist_name")
        
        # Overall lineup score based on valid artists
        if valid_artists > 0:
            score += 0.3 * (valid_artists / len(lineup))
        
        # Bonus for multiple artists
        if len(lineup) >= 3:
            score += 0.2
        elif len(lineup) >= 2:
            score += 0.1
        
        # Has at least one headliner
        if any(artist.get("headliner") for artist in lineup):
            score += 0.1
        
        return min(score, 1.0), {
            "confidence": min(score, 1.0),
            "flags": flags,
            "lastChecked": datetime.utcnow(),
            "itemValidation": item_validation
        }
    
    def _score_ticket_info(self, ticket_info: Dict) -> Tuple[float, Dict]:
        """Score ticket information field"""
        score = 0.0
        flags = []
        
        if not ticket_info:
            return 0.0, {
                "confidence": 0.0,
                "flags": ["missing_ticket_info"],
                "lastChecked": datetime.utcnow()
            }
        
        # Status
        if ticket_info.get("status"):
            score += 0.3
            if ticket_info["status"] in ["available", "sold_out", "coming_soon"]:
                score += 0.1
            else:
                flags.append("invalid_ticket_status")
        else:
            flags.append("missing_ticket_status")
        
        # Price information
        if ticket_info.get("startingPrice") is not None:
            score += 0.2
            # Reasonable price range for Ibiza events
            if 20 <= ticket_info["startingPrice"] <= 200:
                score += 0.1
            else:
                flags.append("unusual_price_range")
        
        # Currency
        if ticket_info.get("currency"):
            score += 0.1
            if ticket_info["currency"] == "EUR":
                score += 0.05
        
        # Ticket URL
        if ticket_info.get("url"):
            score += 0.2
            # Check if URL is valid
            if ticket_info["url"].startswith(("http://", "https://")):
                score += 0.05
            else:
                flags.append("invalid_ticket_url")
        
        # Provider
        if ticket_info.get("provider"):
            score += 0.1
        
        return min(score, 1.0), {
            "confidence": min(score, 1.0),
            "flags": flags,
            "lastChecked": datetime.utcnow()
        }
    
    def _calculate_overall_score(self, field_scores: Dict[str, float]) -> float:
        """Calculate weighted overall score"""
        total_score = 0.0
        total_weight = 0.0
        
        for field, score in field_scores.items():
            if field in self.field_weights:
                total_score += score * self.field_weights[field]
                total_weight += self.field_weights[field]
        
        if total_weight > 0:
            return round(total_score / total_weight, 3)
        return 0.0
    
    def get_quality_summary(self, quality_data: Dict) -> Dict[str, Any]:
        """Generate a human-readable quality summary"""
        overall = quality_data["_quality"]["overall"]
        scores = quality_data["_quality"]["scores"]
        
        # Determine quality level
        if overall >= 0.9:
            quality_level = "Excellent"
        elif overall >= 0.8:
            quality_level = "Good"
        elif overall >= 0.7:
            quality_level = "Fair"
        elif overall >= 0.6:
            quality_level = "Poor"
        else:
            quality_level = "Very Poor"
        
        # Find weakest fields
        weak_fields = [field for field, score in scores.items() if score < 0.7]
        
        # Count total flags
        total_flags = 0
        for field_validation in quality_data["_validation"].values():
            if isinstance(field_validation, dict) and "flags" in field_validation:
                total_flags += len(field_validation["flags"])
        
        return {
            "qualityLevel": quality_level,
            "overallScore": overall,
            "weakFields": weak_fields,
            "totalFlags": total_flags,
            "recommendation": self._get_recommendation(overall, weak_fields)
        }
    
    def _get_recommendation(self, overall_score: float, weak_fields: List[str]) -> str:
        """Generate improvement recommendation"""
        if overall_score >= 0.9:
            return "Data quality is excellent. No immediate action needed."
        elif overall_score >= 0.8:
            return f"Good data quality. Consider improving: {', '.join(weak_fields)}"
        elif overall_score >= 0.7:
            return f"Fair data quality. Priority improvements needed for: {', '.join(weak_fields)}"
        else:
            return "Poor data quality. Consider re-scraping with different extraction method."


# Example usage
if __name__ == "__main__":
    scorer = QualityScorer()
    
    # Test with sample event data
    sample_event = {
        "title": "Glitterbox 25th May 2025",
        "location": {
            "venue": "Hï Ibiza",
            "address": "Platja d'en Bossa",
            "city": "Ibiza",
            "coordinates": {"lat": 38.8827, "lng": 1.4091}
        },
        "dateTime": {
            "start": datetime(2025, 5, 25, 23, 0),
            "displayText": "Sun 25 May 2025",
            "timezone": "Europe/Madrid"
        },
        "lineUp": [
            {"name": "Glitterbox", "headliner": True, "genre": "House/Disco"}
        ],
        "ticketInfo": {
            "status": "available",
            "startingPrice": 45.0,
            "currency": "EUR",
            "url": "https://ticketsibiza.com/tickets/glitterbox"
        }
    }
    
    quality_data = scorer.calculate_event_quality(sample_event)
    summary = scorer.get_quality_summary(quality_data)
    
    print(f"Overall Score: {quality_data['_quality']['overall']}")
    print(f"Quality Level: {summary['qualityLevel']}")
    print(f"Recommendation: {summary['recommendation']}")