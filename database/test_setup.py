#!/usr/bin/env python3
"""
Test script to verify MongoDB setup and demonstrate functionality
"""

import sys
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import json

# Import our modules
from mongodb_setup import MongoDBSetup
from quality_scorer import QualityScorer
from data_migration import DataMigration


def test_mongodb_connection():
    """Test basic MongoDB connection"""
    print("1. Testing MongoDB Connection...")
    try:
        client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        print("   ✅ MongoDB is running and accessible")
        client.close()
        return True
    except ConnectionFailure:
        print("   ❌ MongoDB is not running or not accessible")
        print("   Please start MongoDB with: sudo systemctl start mongod")
        return False


def test_database_setup():
    """Test database setup"""
    print("\n2. Testing Database Setup...")
    setup = MongoDBSetup()
    
    if not setup.connect():
        print("   ❌ Failed to connect to database")
        return False
    
    # Create collections
    setup.create_collections()
    
    # Verify setup
    verification = setup.verify_setup()
    
    print("   Database:", verification["database"])
    print("   Collections created:")
    for collection, exists in verification["collections"].items():
        status = "✅" if exists else "❌"
        indexes = verification["indexes"].get(collection, 0)
        print(f"     {status} {collection} ({indexes} indexes)")
    
    setup.close()
    return all(verification["collections"].values())


def test_quality_scorer():
    """Test quality scoring functionality"""
    print("\n3. Testing Quality Scorer...")
    scorer = QualityScorer()
    
    # Test with good quality event
    good_event = {
        "title": "Carl Cox at Privilege Ibiza - 15th July 2025",
        "location": {
            "venue": "Privilege Ibiza",
            "address": "Carretera Ibiza a San Antonio",
            "city": "Ibiza",
            "country": "Spain",
            "coordinates": {"lat": 38.9784, "lng": 1.4109}
        },
        "dateTime": {
            "start": datetime(2025, 7, 15, 23, 0),
            "end": datetime(2025, 7, 16, 6, 0),
            "displayText": "Tue 15 July 2025",
            "timezone": "Europe/Madrid"
        },
        "lineUp": [
            {"name": "Carl Cox", "headliner": True, "genre": "Techno"},
            {"name": "Adam Beyer", "headliner": False, "genre": "Techno"},
            {"name": "Charlotte de Witte", "headliner": False, "genre": "Techno"}
        ],
        "ticketInfo": {
            "status": "available",
            "startingPrice": 60.0,
            "currency": "EUR",
            "url": "https://ticketsibiza.com/carl-cox-privilege",
            "provider": "Tickets Ibiza"
        }
    }
    
    # Test with poor quality event
    poor_event = {
        "title": "Event",
        "location": {"venue": "Unknown"},
        "dateTime": {},
        "lineUp": [],
        "ticketInfo": {}
    }
    
    # Calculate scores
    good_quality = scorer.calculate_event_quality(good_event)
    poor_quality = scorer.calculate_event_quality(poor_event)
    
    print(f"   Good Event Quality Score: {good_quality['_quality']['overall']:.3f}")
    print(f"   Poor Event Quality Score: {poor_quality['_quality']['overall']:.3f}")
    
    # Get summaries
    good_summary = scorer.get_quality_summary(good_quality)
    poor_summary = scorer.get_quality_summary(poor_quality)
    
    print(f"   Good Event Level: {good_summary['qualityLevel']}")
    print(f"   Poor Event Level: {poor_summary['qualityLevel']}")
    
    return True


def test_sample_data_insertion():
    """Test inserting sample data"""
    print("\n4. Testing Sample Data Insertion...")
    
    client = MongoClient("mongodb://localhost:27017/")
    db = client.tickets_ibiza_events
    scorer = QualityScorer()
    
    sample_events = [
        {
            "url": "https://ticketsibiza.com/event/amnesia-opening-2025/",
            "scrapedAt": datetime.utcnow(),
            "extractionMethod": "jsonld",
            "title": "Amnesia Opening Party 2025",
            "location": {
                "venue": "Amnesia",
                "address": "Carretera Ibiza a San Antonio",
                "city": "San Rafael",
                "country": "Spain"
            },
            "dateTime": {
                "start": datetime(2025, 6, 1, 23, 0),
                "displayText": "Sun 1 June 2025",
                "timezone": "Europe/Madrid"
            },
            "lineUp": [
                {"name": "Marco Carola", "headliner": True, "genre": "Techno"}
            ],
            "ticketInfo": {
                "status": "available",
                "startingPrice": 50.0,
                "currency": "EUR"
            }
        },
        {
            "url": "https://ticketsibiza.com/event/dc10-circoloco-2025/",
            "scrapedAt": datetime.utcnow(),
            "extractionMethod": "html_parsing",
            "title": "DC10 Circoloco Monday",
            "location": {
                "venue": "DC10",
                "city": "Ibiza"
            },
            "dateTime": {
                "start": datetime(2025, 7, 7, 16, 0),
                "displayText": "Mon 7 July 2025"
            },
            "lineUp": [
                {"name": "Seth Troxler", "headliner": True}
            ],
            "ticketInfo": {
                "status": "sold_out"
            }
        }
    ]
    
    inserted = 0
    for event in sample_events:
        # Calculate quality
        quality_data = scorer.calculate_event_quality(event)
        event.update(quality_data)
        
        # Insert
        try:
            result = db.events.update_one(
                {"url": event["url"]},
                {"$set": event},
                upsert=True
            )
            if result.upserted_id or result.modified_count:
                inserted += 1
                print(f"   ✅ Inserted: {event['title']} (Quality: {event['_quality']['overall']:.2f})")
        except Exception as e:
            print(f"   ❌ Failed to insert {event['title']}: {e}")
    
    # Check total count
    total_events = db.events.count_documents({})
    print(f"   Total events in database: {total_events}")
    
    client.close()
    return inserted > 0


def test_quality_queries():
    """Test querying events by quality"""
    print("\n5. Testing Quality-Based Queries...")
    
    client = MongoClient("mongodb://localhost:27017/")
    db = client.tickets_ibiza_events
    
    # High quality events
    high_quality = list(db.events.find(
        {"_quality.overall": {"$gte": 0.8}},
        {"title": 1, "_quality.overall": 1}
    ).limit(5))
    
    print(f"   High Quality Events (>= 0.8): {len(high_quality)}")
    for event in high_quality[:3]:
        print(f"     - {event['title']}: {event['_quality']['overall']:.3f}")
    
    # Events with issues
    with_issues = db.events.count_documents({
        "$or": [
            {"_validation.title.flags": {"$ne": []}},
            {"_validation.location.flags": {"$ne": []}},
            {"_validation.dateTime.flags": {"$ne": []}}
        ]
    })
    
    print(f"   Events with validation issues: {with_issues}")
    
    # Quality distribution
    pipeline = [
        {
            "$group": {
                "_id": None,
                "total": {"$sum": 1},
                "avgQuality": {"$avg": "$_quality.overall"},
                "excellent": {"$sum": {"$cond": [{"$gte": ["$_quality.overall", 0.9]}, 1, 0]}},
                "good": {"$sum": {"$cond": [
                    {"$and": [
                        {"$gte": ["$_quality.overall", 0.8]},
                        {"$lt": ["$_quality.overall", 0.9]}
                    ]}, 1, 0
                ]}},
                "fair": {"$sum": {"$cond": [
                    {"$and": [
                        {"$gte": ["$_quality.overall", 0.7]},
                        {"$lt": ["$_quality.overall", 0.8]}
                    ]}, 1, 0
                ]}},
                "poor": {"$sum": {"$cond": [{"$lt": ["$_quality.overall", 0.7]}, 1, 0]}}
            }
        }
    ]
    
    result = list(db.events.aggregate(pipeline))
    if result:
        stats = result[0]
        print(f"\n   Quality Distribution:")
        print(f"     Total Events: {stats['total']}")
        print(f"     Average Quality: {stats['avgQuality']:.3f}")
        print(f"     Excellent (≥0.9): {stats['excellent']}")
        print(f"     Good (0.8-0.9): {stats['good']}")
        print(f"     Fair (0.7-0.8): {stats['fair']}")
        print(f"     Poor (<0.7): {stats['poor']}")
    
    client.close()
    return True


def main():
    """Run all tests"""
    print("="*60)
    print("MongoDB Event Data Quality System - Test Suite")
    print("="*60)
    
    # Run tests
    tests = [
        ("MongoDB Connection", test_mongodb_connection),
        ("Database Setup", test_database_setup),
        ("Quality Scorer", test_quality_scorer),
        ("Sample Data Insertion", test_sample_data_insertion),
        ("Quality Queries", test_quality_queries)
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        try:
            if test_func():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"   ❌ Error in {test_name}: {e}")
            failed += 1
    
    # Summary
    print("\n" + "="*60)
    print(f"Test Summary: {passed} passed, {failed} failed")
    print("="*60)
    
    if failed == 0:
        print("\n✅ All tests passed! The system is ready to use.")
        print("\nNext steps:")
        print("1. Run 'python data_migration.py' to migrate your existing data")
        print("2. Check the README.md for integration examples")
        print("3. Start building your API endpoints")
    else:
        print("\n❌ Some tests failed. Please check the errors above.")
        if not test_mongodb_connection():
            print("\nMake sure MongoDB is installed and running:")
            print("  Ubuntu/Debian: sudo systemctl start mongod")
            print("  macOS: brew services start mongodb-community")
            print("  Docker: docker run -d -p 27017:27017 mongo:latest")


if __name__ == "__main__":
    main()