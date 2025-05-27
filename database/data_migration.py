"""
Data Migration Script for Tickets Ibiza Event Data
Consolidates data from JSON files into MongoDB with quality scoring
"""

import json
import os
from datetime import datetime
from typing import Dict, List, Any, Optional
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
import logging
import re
from pathlib import Path

from mongodb_setup import MongoDBSetup
from quality_scorer import QualityScorer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataMigration:
    """Handles migration of event data from JSON files to MongoDB"""
    
    def __init__(self, db_connection_string: str = "mongodb://localhost:27017/",
                 database_name: str = "tickets_ibiza_events"):
        """Initialize migration with database connection"""
        self.client = MongoClient(db_connection_string)
        self.db = self.client[database_name]
        self.scorer = QualityScorer()
        self.stats = {
            "total_processed": 0,
            "successfully_migrated": 0,
            "duplicates_found": 0,
            "errors": 0,
            "quality_scores": []
        }
    
    def load_json_file(self, filepath: str) -> Optional[Dict]:
        """Load and parse JSON file"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading {filepath}: {e}")
            return None
    
    def parse_event_from_scraped_data(self, event_data: Dict) -> Dict:
        """Parse event from ticketsibiza_scraped_data.json format"""
        parsed_event = {
            "url": event_data.get("url", ""),
            "scrapedAt": datetime.utcnow(),
            "extractionMethod": event_data.get("extractionMethod", "unknown"),
            "title": event_data.get("title", ""),
            "fullDescription": event_data.get("fullDescription", ""),
            "images": event_data.get("images", [])
        }
        
        # Parse location
        location_data = {}
        if event_data.get("location"):
            # Handle both string and object location formats
            if isinstance(event_data["location"], dict):
                location_data = event_data["location"]
                # Extract venue name if nested
                if "venue" in location_data and isinstance(location_data["venue"], dict):
                    location_data["venue"] = location_data["venue"].get("venue", "")
            else:
                location_data["venue"] = event_data["location"]
            
            # Try to extract venue details from location string
            venue_str = str(location_data.get("venue", ""))
            if venue_str and "Hï Ibiza" in venue_str:
                location_data.update({
                    "address": "Platja d'en Bossa",
                    "city": "Ibiza",
                    "country": "Spain"
                })
            elif "Ushuaïa" in venue_str:
                location_data.update({
                    "address": "Platja d'en Bossa",
                    "city": "Ibiza",
                    "country": "Spain"
                })
            elif "Pacha" in venue_str:
                location_data.update({
                    "address": "Av. 8 d'Agost",
                    "city": "Ibiza Town",
                    "country": "Spain"
                })
            
            # Ensure we have basic location info
            if "city" not in location_data:
                location_data["city"] = "Ibiza"
            if "country" not in location_data:
                location_data["country"] = "Spain"
        parsed_event["location"] = location_data
        
        # Parse date/time
        datetime_data = {}
        if event_data.get("dateTime"):
            dt_info = event_data["dateTime"]
            if dt_info.get("start"):
                try:
                    # Handle ISO format
                    start_dt = datetime.fromisoformat(dt_info["start"].replace('Z', '+00:00'))
                    datetime_data["start"] = start_dt
                except:
                    pass
            
            if dt_info.get("end"):
                try:
                    end_dt = datetime.fromisoformat(dt_info["end"].replace('Z', '+00:00'))
                    datetime_data["end"] = end_dt
                except:
                    pass
            
            datetime_data["displayText"] = dt_info.get("displayText", "")
            datetime_data["timezone"] = "Europe/Madrid"
        parsed_event["dateTime"] = datetime_data
        
        # Parse lineup
        lineup = []
        if event_data.get("lineUp"):
            for idx, artist in enumerate(event_data["lineUp"]):
                artist_info = {
                    "name": artist.get("name", ""),
                    "headliner": artist.get("headliner", idx == 0),  # First is headliner by default
                }
                # Try to determine genre from artist name or event title
                if "techno" in parsed_event["title"].lower():
                    artist_info["genre"] = "Techno"
                elif "house" in parsed_event["title"].lower():
                    artist_info["genre"] = "House"
                elif "glitterbox" in artist.get("name", "").lower():
                    artist_info["genre"] = "House/Disco"
                
                lineup.append(artist_info)
        parsed_event["lineUp"] = lineup
        
        # Parse ticket info
        ticket_info = {}
        if event_data.get("ticketInfo"):
            ti = event_data["ticketInfo"]
            ticket_info = {
                "status": "sold_out" if ti.get("isSoldOut") else "available",
                "startingPrice": ti.get("startingPrice"),
                "currency": ti.get("currency", "EUR"),
                "url": ti.get("url", event_data.get("ticketsUrl", "")),
                "provider": "Tickets Ibiza"
            }
        elif event_data.get("ticketsUrl"):
            ticket_info = {
                "status": "available",
                "url": event_data["ticketsUrl"],
                "currency": "EUR",
                "provider": "Tickets Ibiza"
            }
        parsed_event["ticketInfo"] = ticket_info
        
        return parsed_event
    
    def deduplicate_events(self, events: List[Dict]) -> List[Dict]:
        """Remove duplicate events based on URL and date"""
        seen = set()
        unique_events = []
        
        for event in events:
            # Create unique key from URL and date
            key = event.get("url", "")
            if event.get("dateTime", {}).get("start"):
                key += str(event["dateTime"]["start"])
            
            if key not in seen:
                seen.add(key)
                unique_events.append(event)
            else:
                self.stats["duplicates_found"] += 1
                logger.info(f"Duplicate found: {event.get('title', 'Unknown')}")
        
        return unique_events
    
    def migrate_events(self, events: List[Dict], batch_size: int = 100):
        """Migrate events to MongoDB in batches"""
        logger.info(f"Starting migration of {len(events)} events")
        
        for i in range(0, len(events), batch_size):
            batch = events[i:i + batch_size]
            operations = []
            
            for event in batch:
                self.stats["total_processed"] += 1
                
                # Calculate quality scores
                quality_data = self.scorer.calculate_event_quality(event)
                event.update(quality_data)
                
                # Track quality scores
                self.stats["quality_scores"].append(quality_data["_quality"]["overall"])
                
                # Create upsert operation
                operations.append(
                    UpdateOne(
                        {"url": event["url"]},
                        {"$set": event},
                        upsert=True
                    )
                )
            
            # Execute batch operation
            try:
                result = self.db.events.bulk_write(operations)
                self.stats["successfully_migrated"] += result.modified_count + result.upserted_count
                logger.info(f"Batch {i//batch_size + 1}: Migrated {result.modified_count + result.upserted_count} events")
            except BulkWriteError as e:
                logger.error(f"Batch write error: {e}")
                self.stats["errors"] += len(batch)
    
    def migrate_from_json_files(self, json_file_path: str, parsed_md_path: Optional[str] = None):
        """Main migration function"""
        logger.info("Starting data migration process")
        
        # Load main JSON file
        scraped_data = self.load_json_file(json_file_path)
        if not scraped_data:
            logger.error("Failed to load scraped data JSON file")
            return
        
        # Parse events
        events = []
        if isinstance(scraped_data, list):
            for event_data in scraped_data:
                parsed_event = self.parse_event_from_scraped_data(event_data)
                events.append(parsed_event)
        elif isinstance(scraped_data, dict) and "events" in scraped_data:
            for event_data in scraped_data["events"]:
                parsed_event = self.parse_event_from_scraped_data(event_data)
                events.append(parsed_event)
        
        logger.info(f"Parsed {len(events)} events from JSON file")
        
        # Deduplicate
        unique_events = self.deduplicate_events(events)
        logger.info(f"Found {len(unique_events)} unique events after deduplication")
        
        # Migrate to MongoDB
        self.migrate_events(unique_events)
        
        # Print summary
        self.print_migration_summary()
    
    def print_migration_summary(self):
        """Print migration statistics"""
        print("\n" + "="*50)
        print("MIGRATION SUMMARY")
        print("="*50)
        print(f"Total events processed: {self.stats['total_processed']}")
        print(f"Successfully migrated: {self.stats['successfully_migrated']}")
        print(f"Duplicates found: {self.stats['duplicates_found']}")
        print(f"Errors: {self.stats['errors']}")
        
        if self.stats['quality_scores']:
            avg_quality = sum(self.stats['quality_scores']) / len(self.stats['quality_scores'])
            print(f"\nAverage quality score: {avg_quality:.3f}")
            print(f"Highest quality score: {max(self.stats['quality_scores']):.3f}")
            print(f"Lowest quality score: {min(self.stats['quality_scores']):.3f}")
        
        print("="*50)
    
    def create_quality_report(self) -> Dict[str, Any]:
        """Generate detailed quality report for migrated data"""
        pipeline = [
            {
                "$group": {
                    "_id": None,
                    "totalEvents": {"$sum": 1},
                    "avgQuality": {"$avg": "$_quality.overall"},
                    "excellentQuality": {
                        "$sum": {"$cond": [{"$gte": ["$_quality.overall", 0.9]}, 1, 0]}
                    },
                    "goodQuality": {
                        "$sum": {"$cond": [
                            {"$and": [
                                {"$gte": ["$_quality.overall", 0.8]},
                                {"$lt": ["$_quality.overall", 0.9]}
                            ]}, 1, 0
                        ]}
                    },
                    "fairQuality": {
                        "$sum": {"$cond": [
                            {"$and": [
                                {"$gte": ["$_quality.overall", 0.7]},
                                {"$lt": ["$_quality.overall", 0.8]}
                            ]}, 1, 0
                        ]}
                    },
                    "poorQuality": {
                        "$sum": {"$cond": [{"$lt": ["$_quality.overall", 0.7]}, 1, 0]}
                    }
                }
            }
        ]
        
        result = list(self.db.events.aggregate(pipeline))
        
        if result:
            report = result[0]
            report["qualityDistribution"] = {
                "excellent": report.pop("excellentQuality", 0),
                "good": report.pop("goodQuality", 0),
                "fair": report.pop("fairQuality", 0),
                "poor": report.pop("poorQuality", 0)
            }
            return report
        
        return {"error": "No data found"}
    
    def close(self):
        """Close database connection"""
        self.client.close()


def main():
    """Main migration function"""
    # File paths - adjust these to your actual file locations
    json_file_path = "../ticketsibiza_scraped_data.json"
    
    # Check if files exist
    if not os.path.exists(json_file_path):
        logger.error(f"JSON file not found: {json_file_path}")
        logger.info("Please ensure the file path is correct")
        return
    
    # Initialize migration
    migration = DataMigration()
    
    try:
        # Run migration
        migration.migrate_from_json_files(json_file_path)
        
        # Generate quality report
        print("\nGenerating quality report...")
        report = migration.create_quality_report()
        
        if "error" not in report:
            print("\nQUALITY REPORT")
            print("="*50)
            print(f"Total events in database: {report.get('totalEvents', 0)}")
            print(f"Average quality score: {report.get('avgQuality', 0):.3f}")
            print("\nQuality Distribution:")
            dist = report.get('qualityDistribution', {})
            for level, count in dist.items():
                print(f"  {level.capitalize()}: {count} events")
        
    finally:
        migration.close()


if __name__ == "__main__":
    main()