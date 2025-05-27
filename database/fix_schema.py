"""
Fix MongoDB schema to be more flexible for data migration
"""

from pymongo import MongoClient
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def fix_schema():
    """Update the MongoDB schema to be more flexible"""
    client = MongoClient("mongodb://localhost:27017/")
    db = client.tickets_ibiza_events
    
    # More flexible validation schema
    validation_schema = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["url", "scrapedAt"],
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
                    "description": "Method used to extract data"
                },
                "title": {
                    "bsonType": "string",
                    "description": "Event title"
                },
                "location": {
                    "bsonType": "object",
                    "description": "Location information"
                },
                "dateTime": {
                    "bsonType": "object",
                    "description": "Date and time information"
                },
                "lineUp": {
                    "bsonType": "array",
                    "description": "Artist lineup"
                },
                "ticketInfo": {
                    "bsonType": "object",
                    "description": "Ticket information"
                },
                "_quality": {
                    "bsonType": "object",
                    "description": "Quality metadata"
                },
                "_validation": {
                    "bsonType": "object",
                    "description": "Validation tracking data"
                }
            }
        }
    }
    
    try:
        # Remove existing validation
        db.command("collMod", "events", validator={})
        logger.info("Removed existing validation")
        
        # Apply new flexible validation
        db.command("collMod", "events", validator=validation_schema)
        logger.info("Applied flexible validation schema to 'events' collection")
        
        print("✅ Schema updated successfully!")
        print("The database is now ready for data migration.")
        
    except Exception as e:
        logger.error(f"Error updating schema: {e}")
    finally:
        client.close()


if __name__ == "__main__":
    fix_schema()