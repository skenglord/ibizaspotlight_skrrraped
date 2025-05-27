# MongoDB Event Data Quality System

This directory contains the implementation of a MongoDB-based data quality scoring system for Tickets Ibiza event data.

## Prerequisites

1. **MongoDB Installation**
   - Install MongoDB Community Edition: https://www.mongodb.com/docs/manual/installation/
   - Ensure MongoDB is running locally on default port 27017
   - Or use Docker: `docker run -d -p 27017:27017 --name mongodb mongo:latest`

2. **Python Dependencies**
   ```bash
   cd skrrraped_graph/database
   pip install -r requirements.txt
   ```

## Quick Start

### 1. Set Up MongoDB Database

First, ensure MongoDB is running, then set up the database schema:

```bash
python mongodb_setup.py
```

This will:
- Create the `tickets_ibiza_events` database
- Set up collections with proper schemas and indexes:
  - `events` - Main event data with quality metadata
  - `quality_scores` - Historical quality score tracking
  - `validation_history` - Validation attempt logs
  - `extraction_methods` - Extraction method effectiveness tracking
- Insert sample data for testing

### 2. Migrate Existing Data

To migrate your existing JSON data to MongoDB:

```bash
python data_migration.py
```

This will:
- Load data from `ticketsibiza_scraped_data.json`
- Parse and transform the data to match the new schema
- Calculate quality scores for each event
- Remove duplicates based on URL and date
- Insert data into MongoDB with quality metadata

### 3. Verify Setup

Check that everything is working:

```python
from mongodb_setup import MongoDBSetup

setup = MongoDBSetup()
setup.connect()
verification = setup.verify_setup()
print(verification)
setup.close()
```

## Database Schema

### Events Collection

```javascript
{
  "_id": ObjectId,
  "url": "https://ticketsibiza.com/event/...",
  "scrapedAt": ISODate,
  "extractionMethod": "jsonld|html_parsing|mixed|manual",
  
  // Core event data
  "title": "Event Title",
  "location": {
    "venue": "Hï Ibiza",
    "address": "Street Address",
    "city": "Ibiza",
    "country": "Spain",
    "coordinates": {
      "lat": 38.8827,
      "lng": 1.4091
    }
  },
  "dateTime": {
    "start": ISODate,
    "end": ISODate,
    "displayText": "Formatted date string",
    "timezone": "Europe/Madrid"
  },
  "lineUp": [
    {
      "name": "Artist Name",
      "headliner": true,
      "genre": "House/Techno",
      "startTime": "23:00"
    }
  ],
  "ticketInfo": {
    "status": "available|sold_out|coming_soon",
    "startingPrice": 45.0,
    "currency": "EUR",
    "url": "https://...",
    "provider": "Tickets Ibiza"
  },
  
  // Quality metadata
  "_quality": {
    "scores": {
      "title": 0.95,
      "location": 0.90,
      "dateTime": 0.95,
      "lineUp": 0.85,
      "ticketInfo": 0.88
    },
    "overall": 0.91,
    "lastCalculated": ISODate
  },
  
  // Validation tracking
  "_validation": {
    "title": {
      "method": "jsonld",
      "confidence": 0.95,
      "lastChecked": ISODate,
      "flags": []
    }
    // ... other fields
  }
}
```

## Quality Scoring System

The quality scorer evaluates each field based on:

### Title (25% weight)
- Minimum length requirements
- Date pattern presence
- Proper capitalization
- Special character ratio

### Location (20% weight)
- Venue name presence
- Known Ibiza venues bonus
- Address completeness
- Valid coordinates for Ibiza area

### DateTime (25% weight)
- Start date presence and validity
- Date range reasonability
- Display text formatting
- Timezone accuracy

### LineUp (15% weight)
- Artist count and completeness
- Name quality validation
- Headliner designation
- Genre information

### TicketInfo (15% weight)
- Status validity
- Price range reasonability
- URL validation
- Currency and provider info

## Usage Examples

### Query High-Quality Events

```python
from pymongo import MongoClient

client = MongoClient()
db = client.tickets_ibiza_events

# Find all events with quality score > 0.8
high_quality_events = db.events.find({
    "_quality.overall": {"$gte": 0.8}
}).sort("dateTime.start", 1)

for event in high_quality_events:
    print(f"{event['title']} - Quality: {event['_quality']['overall']}")
```

### Get Events with Issues

```python
# Find events with location issues
events_with_location_issues = db.events.find({
    "_validation.location.flags": {"$ne": []}
})

# Find events missing lineup info
events_missing_lineup = db.events.find({
    "_quality.scores.lineUp": {"$lt": 0.5}
})
```

### Calculate Quality Statistics

```python
# Aggregate quality statistics
pipeline = [
    {
        "$group": {
            "_id": None,
            "avgQuality": {"$avg": "$_quality.overall"},
            "minQuality": {"$min": "$_quality.overall"},
            "maxQuality": {"$max": "$_quality.overall"}
        }
    }
]

stats = list(db.events.aggregate(pipeline))[0]
print(f"Average Quality: {stats['avgQuality']:.3f}")
```

## Integration with Scraper

To integrate with your existing scraper (`mono_ticketmaster.py`), add quality scoring:

```python
from database.quality_scorer import QualityScorer
from pymongo import MongoClient

# In your scraper
scorer = QualityScorer()
client = MongoClient()
db = client.tickets_ibiza_events

# After scraping event data
event_data = scraper.scrape_event_data(url)
quality_data = scorer.calculate_event_quality(event_data)
event_data.update(quality_data)

# Insert/update in MongoDB
db.events.update_one(
    {"url": event_data["url"]},
    {"$set": event_data},
    upsert=True
)
```

## Monitoring & Maintenance

### Check Database Health

```bash
# MongoDB shell
mongosh tickets_ibiza_events --eval "db.stats()"
```

### Export Data

```bash
# Export high-quality events to JSON
mongoexport --db=tickets_ibiza_events --collection=events \
  --query='{"_quality.overall": {"$gte": 0.8}}' \
  --out=high_quality_events.json
```

### Backup Database

```bash
# Backup entire database
mongodump --db=tickets_ibiza_events --out=backup/

# Restore from backup
mongorestore --db=tickets_ibiza_events backup/tickets_ibiza_events/
```

## Troubleshooting

1. **Connection Failed**
   - Ensure MongoDB is running: `sudo systemctl status mongod`
   - Check if port 27017 is available: `netstat -an | grep 27017`

2. **Import Errors**
   - Install dependencies: `pip install -r requirements.txt`
   - Check Python version (3.7+ required)

3. **Data Quality Issues**
   - Review events with low scores
   - Check validation flags for specific issues
   - Consider adjusting scraping methods for problematic fields

## Next Steps

1. Set up automated quality monitoring
2. Implement API endpoints for your app
3. Create quality improvement workflows
4. Add ML-based quality prediction
5. Set up real-time quality alerts

For more details, see the comprehensive implementation plan in `comprehensive_event_data_and_quality_plan.md`.