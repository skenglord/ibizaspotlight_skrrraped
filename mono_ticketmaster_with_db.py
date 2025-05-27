"""
Enhanced Ticketmaster/TicketsIbiza scraper with MongoDB integration and quality scoring
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# Import the original scraper
from mono_ticketmaster import MultiLayerEventScraper

# Import our database modules
from database.quality_scorer import QualityScorer
from database.mongodb_setup import MongoDBSetup

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MongoIntegratedEventScraper(MultiLayerEventScraper):
    """Enhanced scraper that saves directly to MongoDB with quality scoring"""
    
    def __init__(self, use_browser: bool = True, 
                 db_connection: str = "mongodb://localhost:27017/",
                 database_name: str = "tickets_ibiza_events"):
        """
        Initialize scraper with MongoDB integration
        
        Args:
            use_browser: Whether to use browser for dynamic content
            db_connection: MongoDB connection string
            database_name: Name of the database to use
        """
        super().__init__(use_browser)
        
        # Initialize database connection
        self.db_client = None
        self.db = None
        self.scorer = QualityScorer()
        
        try:
            self.db_client = MongoClient(db_connection)
            self.db_client.admin.command('ping')
            self.db = self.db_client[database_name]
            logger.info(f"Connected to MongoDB database: {database_name}")
        except ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            self.db = None
    
    def scrape_and_save_event(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Scrape event data and save to MongoDB with quality scoring
        
        Args:
            url: Event URL to scrape
            
        Returns:
            Event data with quality scores if successful, None otherwise
        """
        # Scrape the event
        event_data = self.scrape_event_data(url)
        
        if not event_data:
            logger.error(f"Failed to scrape data from {url}")
            return None
        
        # Add scraping metadata
        event_data['scrapedAt'] = datetime.utcnow()
        event_data['lastUpdated'] = datetime.utcnow()
        
        # Calculate quality scores
        quality_data = self.scorer.calculate_event_quality(event_data)
        event_data.update(quality_data)
        
        # Get quality summary
        summary = self.scorer.get_quality_summary(quality_data)
        
        logger.info(f"Scraped: {event_data.get('title', 'Unknown')}")
        logger.info(f"Quality: {summary['qualityLevel']} ({quality_data['_quality']['overall']:.3f})")
        
        # Save to MongoDB if connected
        if self.db:
            try:
                result = self.db.events.update_one(
                    {"url": url},
                    {
                        "$set": event_data,
                        "$setOnInsert": {"firstScraped": datetime.utcnow()}
                    },
                    upsert=True
                )
                
                if result.upserted_id:
                    logger.info(f"Inserted new event with ID: {result.upserted_id}")
                elif result.modified_count:
                    logger.info(f"Updated existing event")
                
                # Track extraction method effectiveness
                self._update_extraction_method_stats(event_data)
                
                # Save quality score history
                self._save_quality_history(url, quality_data)
                
            except Exception as e:
                logger.error(f"Failed to save to MongoDB: {e}")
        
        return event_data
    
    def scrape_multiple_events(self, urls: List[str], 
                             save_to_file: bool = False) -> Dict[str, Any]:
        """
        Scrape multiple events and save to MongoDB
        
        Args:
            urls: List of event URLs to scrape
            save_to_file: Whether to also save results to JSON file
            
        Returns:
            Summary of scraping results
        """
        results = {
            "total": len(urls),
            "successful": 0,
            "failed": 0,
            "quality_scores": [],
            "events": []
        }
        
        for i, url in enumerate(urls, 1):
            logger.info(f"\nProcessing {i}/{len(urls)}: {url}")
            
            try:
                event_data = self.scrape_and_save_event(url)
                
                if event_data:
                    results["successful"] += 1
                    results["quality_scores"].append(event_data["_quality"]["overall"])
                    
                    # Remove internal fields for cleaner output
                    clean_data = {k: v for k, v in event_data.items() 
                                if not k.startswith("_")}
                    results["events"].append(clean_data)
                else:
                    results["failed"] += 1
                    
            except Exception as e:
                logger.error(f"Error processing {url}: {e}")
                results["failed"] += 1
        
        # Calculate statistics
        if results["quality_scores"]:
            results["avg_quality"] = sum(results["quality_scores"]) / len(results["quality_scores"])
            results["min_quality"] = min(results["quality_scores"])
            results["max_quality"] = max(results["quality_scores"])
        
        # Save to file if requested
        if save_to_file:
            filename = f"scraping_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, default=str)
            logger.info(f"Results saved to {filename}")
        
        # Print summary
        self._print_summary(results)
        
        return results
    
    def _update_extraction_method_stats(self, event_data: Dict):
        """Update extraction method effectiveness statistics"""
        if not self.db:
            return
        
        method = event_data.get("extractionMethod", "unknown")
        quality_score = event_data["_quality"]["overall"]
        
        try:
            self.db.extraction_methods.update_one(
                {"method": method},
                {
                    "$inc": {"totalUses": 1},
                    "$push": {"qualityScores": quality_score},
                    "$set": {"lastUsed": datetime.utcnow()}
                },
                upsert=True
            )
        except Exception as e:
            logger.error(f"Failed to update extraction method stats: {e}")
    
    def _save_quality_history(self, url: str, quality_data: Dict):
        """Save quality score history for tracking improvements"""
        if not self.db:
            return
        
        try:
            history_entry = {
                "eventUrl": url,
                "calculatedAt": datetime.utcnow(),
                "overallScore": quality_data["_quality"]["overall"],
                "fieldScores": quality_data["_quality"]["scores"],
                "validationFlags": {
                    field: len(data.get("flags", [])) 
                    for field, data in quality_data["_validation"].items()
                    if isinstance(data, dict)
                }
            }
            
            self.db.quality_scores.insert_one(history_entry)
        except Exception as e:
            logger.error(f"Failed to save quality history: {e}")
    
    def _print_summary(self, results: Dict):
        """Print scraping summary"""
        print("\n" + "="*60)
        print("SCRAPING SUMMARY")
        print("="*60)
        print(f"Total URLs: {results['total']}")
        print(f"Successful: {results['successful']}")
        print(f"Failed: {results['failed']}")
        
        if results.get("avg_quality"):
            print(f"\nQuality Scores:")
            print(f"  Average: {results['avg_quality']:.3f}")
            print(f"  Minimum: {results['min_quality']:.3f}")
            print(f"  Maximum: {results['max_quality']:.3f}")
        print("="*60)
    
    def get_events_needing_update(self, days_old: int = 7) -> List[str]:
        """
        Get URLs of events that need updating based on age and quality
        
        Args:
            days_old: Number of days before considering data stale
            
        Returns:
            List of event URLs that should be re-scraped
        """
        if not self.db:
            return []
        
        cutoff_date = datetime.utcnow() - timedelta(days=days_old)
        
        # Find events that are either old or have low quality
        events = list(self.db.events.find(
            {
                "$or": [
                    {"lastUpdated": {"$lt": cutoff_date}},
                    {"_quality.overall": {"$lt": 0.7}}
                ]
            },
            {"url": 1, "title": 1, "_quality.overall": 1, "lastUpdated": 1}
        ).sort("_quality.overall", 1).limit(50))
        
        logger.info(f"Found {len(events)} events needing update")
        for event in events[:5]:  # Show first 5
            logger.info(f"  - {event['title'][:50]}... (Quality: {event['_quality']['overall']:.3f})")
        
        return [event["url"] for event in events]
    
    def close(self):
        """Close database connection"""
        if self.db_client:
            self.db_client.close()
            logger.info("MongoDB connection closed")


# Example usage
if __name__ == "__main__":
    # Initialize scraper with MongoDB integration
    scraper = MongoIntegratedEventScraper(use_browser=False)
    
    # Example 1: Scrape a single event
    print("\n1. Scraping single event...")
    url = "https://ticketsibiza.com/event/glitterbox-25th-may-2025/"
    event = scraper.scrape_and_save_event(url)
    
    if event:
        print(f"✓ Scraped: {event['title']}")
        print(f"✓ Quality Score: {event['_quality']['overall']:.3f}")
    
    # Example 2: Scrape multiple events
    print("\n2. Scraping multiple events...")
    urls = [
        "https://ticketsibiza.com/event/circoloco-26th-may-2025/",
        "https://ticketsibiza.com/event/black-coffee-31st-may-2025/",
        "https://ticketsibiza.com/event/calvin-harris-30th-may-2025/"
    ]
    
    results = scraper.scrape_multiple_events(urls, save_to_file=True)
    
    # Example 3: Find events needing update
    print("\n3. Finding events needing update...")
    stale_urls = scraper.get_events_needing_update(days_old=7)
    print(f"Found {len(stale_urls)} events that need updating")
    
    # Clean up
    scraper.close()