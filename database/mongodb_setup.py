"""
MongoDB Setup and Schema Implementation for Event Data Quality System
"""

import os
from datetime import datetime
from typing import Dict, List, Optional, Any
from pymongo import MongoClient, ASCENDING, DESCENDING, IndexModel
from pymongo.errors import ConnectionFailure, OperationFailure
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MongoDBSetup:
    """Handles MongoDB connection and schema setup for event data quality system"""
    
    def __init__(self, connection_string: str = "mongodb://localhost:27017/", 
                 database_name: str = "tickets_ibiza_events"):
        """
        Initialize MongoDB connection
        
        Args:
            connection_string: MongoDB connection string
            database_name: Name of the database to use
        """
        self.connection_string = connection_string
        self.database_name = database_name
        self.client = None
        self.db = None
        
    def connect(self) -> bool:
        """Establish connection to MongoDB"""
        try:
            self.client = MongoClient(self.connection_string)
            # Test connection
            self.client.admin.command('ping')
            self.db = self.client[self.database_name]
            logger.info(f"Successfully connected to MongoDB database: {self.database_name}")
            return True
        except ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            return False
    
    def create_collections(self):
        """Create all required collections with schemas"""
        
        # 1. Events Collection
        self._create_events_collection()
        
        # 2. Quality Scores Collection
        self._create_quality_scores_collection()
        
        # 3. Validation History Collection
        self._create_validation_history_collection()
        
        # 4. Extraction Methods Collection
        self._create_extraction_methods_collection()
        
        logger.info("All collections created successfully")
    
    def _create_events_collection(self):
        """Create events collection with enhanced schema for quality metadata"""
        
        # Create collection if it doesn't exist
        if "events" not in self.db.list_collection_names():
            self.db.create_collection("events")
            logger.info("Created 'events' collection")
        
        # Define indexes
        indexes = [
            IndexModel([("url", ASCENDING)], unique=True),
            IndexModel([("dateTime.start", ASCENDING)]),
            IndexModel([("location.venue", ASCENDING)]),
            IndexModel([("_quality.overall", DESCENDING)]),
            IndexModel([("scrapedAt", DESCENDING)]),
            IndexModel([("title", "text"), ("fullDescription", "text")])
        ]
        
        self.db.events.create_indexes(indexes)
        logger.info("Created indexes for 'events' collection")
        
        # Define validation schema
        validation_schema = {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["url", "scrapedAt", "extractionMethod"],
                "properties": {
                    "url": {
                        "bsonType": "string",
                        "description": "Event URL - required"
                    },
                    "scrapedAt": {
                        "bsonType": "date",
                        "description": "Timestamp when data was scraped - required"
                    },
                    "extractionMethod": {
                        "bsonType": "string",
                        "enum": ["jsonld", "html_parsing", "mixed", "manual"],
                        "description": "Method used to extract data - required"
                    },
                    "title": {
                        "bsonType": "string",
                        "description": "Event title"
                    },
                    "location": {
                        "bsonType": "object",
                        "properties": {
                            "venue": {"bsonType": "string"},
                            "address": {"bsonType": "string"},
                            "city": {"bsonType": "string"},
                            "country": {"bsonType": "string"},
                            "coordinates": {
                                "bsonType": "object",
                                "properties": {
                                    "lat": {"bsonType": "double"},
                                    "lng": {"bsonType": "double"}
                                }
                            }
                        }
                    },
                    "dateTime": {
                        "bsonType": "object",
                        "properties": {
                            "start": {"bsonType": "date"},
                            "end": {"bsonType": ["date", "null"]},
                            "displayText": {"bsonType": "string"},
                            "timezone": {"bsonType": "string"}
                        }
                    },
                    "lineUp": {
                        "bsonType": "array",
                        "items": {
                            "bsonType": "object",
                            "properties": {
                                "name": {"bsonType": "string"},
                                "headliner": {"bsonType": "bool"},
                                "genre": {"bsonType": "string"},
                                "startTime": {"bsonType": ["string", "null"]}
                            }
                        }
                    },
                    "ticketInfo": {
                        "bsonType": "object",
                        "properties": {
                            "status": {
                                "bsonType": "string",
                                "enum": ["available", "sold_out", "coming_soon", "unknown"]
                            },
                            "startingPrice": {"bsonType": ["double", "null"]},
                            "currency": {"bsonType": "string"},
                            "url": {"bsonType": "string"},
                            "provider": {"bsonType": "string"}
                        }
                    },
                    "_quality": {
                        "bsonType": "object",
                        "description": "Quality metadata",
                        "properties": {
                            "scores": {
                                "bsonType": "object",
                                "properties": {
                                    "title": {"bsonType": "double", "minimum": 0, "maximum": 1},
                                    "location": {"bsonType": "double", "minimum": 0, "maximum": 1},
                                    "dateTime": {"bsonType": "double", "minimum": 0, "maximum": 1},
                                    "lineUp": {"bsonType": "double", "minimum": 0, "maximum": 1},
                                    "ticketInfo": {"bsonType": "double", "minimum": 0, "maximum": 1}
                                }
                            },
                            "overall": {"bsonType": "double", "minimum": 0, "maximum": 1},
                            "lastCalculated": {"bsonType": "date"}
                        }
                    },
                    "_validation": {
                        "bsonType": "object",
                        "description": "Validation tracking data"
                    }
                }
            }
        }
        
        # Apply validation
        try:
            self.db.command("collMod", "events", validator=validation_schema)
            logger.info("Applied validation schema to 'events' collection")
        except OperationFailure as e:
            logger.warning(f"Could not apply validation schema: {e}")
    
    def _create_quality_scores_collection(self):
        """Create quality_scores collection for tracking score history"""
        
        if "quality_scores" not in self.db.list_collection_names():
            self.db.create_collection("quality_scores")
            logger.info("Created 'quality_scores' collection")
        
        # Define indexes
        indexes = [
            IndexModel([("eventId", ASCENDING), ("calculatedAt", DESCENDING)]),
            IndexModel([("calculatedAt", DESCENDING)]),
            IndexModel([("overallScore", DESCENDING)])
        ]
        
        self.db.quality_scores.create_indexes(indexes)
        logger.info("Created indexes for 'quality_scores' collection")
    
    def _create_validation_history_collection(self):
        """Create validation_history collection for tracking validation attempts"""
        
        if "validation_history" not in self.db.list_collection_names():
            self.db.create_collection("validation_history")
            logger.info("Created 'validation_history' collection")
        
        # Define indexes
        indexes = [
            IndexModel([("eventId", ASCENDING), ("validatedAt", DESCENDING)]),
            IndexModel([("validatedAt", DESCENDING)]),
            IndexModel([("validationType", ASCENDING)])
        ]
        
        self.db.validation_history.create_indexes(indexes)
        logger.info("Created indexes for 'validation_history' collection")
    
    def _create_extraction_methods_collection(self):
        """Create extraction_methods collection for tracking method effectiveness"""
        
        if "extraction_methods" not in self.db.list_collection_names():
            self.db.create_collection("extraction_methods")
            logger.info("Created 'extraction_methods' collection")
        
        # Define indexes
        indexes = [
            IndexModel([("method", ASCENDING), ("domain", ASCENDING)]),
            IndexModel([("successRate", DESCENDING)]),
            IndexModel([("lastUsed", DESCENDING)])
        ]
        
        self.db.extraction_methods.create_indexes(indexes)
        logger.info("Created indexes for 'extraction_methods' collection")
    
    def insert_sample_data(self):
        """Insert sample event data with quality metadata"""
        
        sample_event = {
            "url": "https://ticketsibiza.com/event/glitterbox-25th-may-2025/",
            "scrapedAt": datetime.utcnow(),
            "extractionMethod": "jsonld",
            "title": "Glitterbox 25th May 2025",
            "location": {
                "venue": "Hï Ibiza",
                "address": "Platja d'en Bossa",
                "city": "Ibiza",
                "country": "Spain",
                "coordinates": {
                    "lat": 38.8827,
                    "lng": 1.4091
                }
            },
            "dateTime": {
                "start": datetime(2025, 5, 25, 23, 0),
                "end": datetime(2025, 5, 26, 6, 0),
                "displayText": "Sun 25 May 2025",
                "timezone": "Europe/Madrid"
            },
            "lineUp": [
                {
                    "name": "Glitterbox",
                    "headliner": True,
                    "genre": "House/Disco"
                }
            ],
            "ticketInfo": {
                "status": "available",
                "startingPrice": 45.0,
                "currency": "EUR",
                "url": "https://ticketsibiza.com/tickets/glitterbox-25-may",
                "provider": "Tickets Ibiza"
            },
            "_quality": {
                "scores": {
                    "title": 0.95,
                    "location": 0.98,
                    "dateTime": 0.97,
                    "lineUp": 0.85,
                    "ticketInfo": 0.92
                },
                "overall": 0.93,
                "lastCalculated": datetime.utcnow()
            },
            "_validation": {
                "title": {
                    "method": "jsonld",
                    "confidence": 0.95,
                    "lastChecked": datetime.utcnow(),
                    "flags": []
                },
                "location": {
                    "method": "jsonld",
                    "confidence": 0.98,
                    "lastChecked": datetime.utcnow(),
                    "flags": []
                }
            }
        }
        
        try:
            result = self.db.events.insert_one(sample_event)
            logger.info(f"Inserted sample event with ID: {result.inserted_id}")
        except Exception as e:
            logger.error(f"Failed to insert sample data: {e}")
    
    def verify_setup(self) -> Dict[str, Any]:
        """Verify that all collections are properly set up"""
        
        collections = self.db.list_collection_names()
        required_collections = ["events", "quality_scores", "validation_history", "extraction_methods"]
        
        verification = {
            "database": self.database_name,
            "collections": {},
            "indexes": {},
            "sample_data": False
        }
        
        for collection in required_collections:
            if collection in collections:
                verification["collections"][collection] = True
                # Get index information
                indexes = list(self.db[collection].list_indexes())
                verification["indexes"][collection] = len(indexes)
            else:
                verification["collections"][collection] = False
                verification["indexes"][collection] = 0
        
        # Check for sample data
        if self.db.events.count_documents({}) > 0:
            verification["sample_data"] = True
        
        return verification
    
    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")


def main():
    """Main function to set up MongoDB for event data quality system"""
    
    # Initialize setup
    setup = MongoDBSetup()
    
    # Connect to MongoDB
    if not setup.connect():
        logger.error("Failed to connect to MongoDB. Please ensure MongoDB is running locally.")
        return
    
    try:
        # Create collections with schemas
        setup.create_collections()
        
        # Insert sample data
        setup.insert_sample_data()
        
        # Verify setup
        verification = setup.verify_setup()
        logger.info(f"Setup verification: {verification}")
        
        print("\n✅ MongoDB setup completed successfully!")
        print(f"Database: {setup.database_name}")
        print("\nCreated collections:")
        for collection, exists in verification["collections"].items():
            if exists:
                print(f"  - {collection} (indexes: {verification['indexes'][collection]})")
        
        if verification["sample_data"]:
            print("\n✅ Sample data inserted")
        
    finally:
        setup.close()


if __name__ == "__main__":
    main()