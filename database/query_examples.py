"""
Example queries for the MongoDB event database with quality scoring
"""

from pymongo import MongoClient
from datetime import datetime, timedelta
import json


def get_high_quality_events(min_score=0.8):
    """Get events with quality score above threshold"""
    client = MongoClient()
    db = client.tickets_ibiza_events
    
    events = list(db.events.find(
        {"_quality.overall": {"$gte": min_score}},
        {
            "title": 1,
            "dateTime.displayText": 1,
            "location.venue": 1,
            "_quality.overall": 1,
            "ticketInfo.status": 1
        }
    ).sort("dateTime.start", 1).limit(10))
    
    print(f"\n🌟 HIGH QUALITY EVENTS (Score >= {min_score}):")
    print("=" * 60)
    for event in events:
        print(f"📅 {event.get('dateTime', {}).get('displayText', 'N/A')}")
        print(f"   📍 {event.get('location', {}).get('venue', 'Unknown venue')}")
        print(f"   🎵 {event['title']}")
        print(f"   ⭐ Quality Score: {event['_quality']['overall']:.3f}")
        print(f"   🎟️  Status: {event.get('ticketInfo', {}).get('status', 'Unknown')}")
        print()
    
    client.close()
    return events


def get_events_by_venue(venue_name):
    """Get all events for a specific venue"""
    client = MongoClient()
    db = client.tickets_ibiza_events
    
    # Case-insensitive search
    events = list(db.events.find(
        {"location.venue": {"$regex": venue_name, "$options": "i"}},
        {
            "title": 1,
            "dateTime.displayText": 1,
            "location.venue": 1,
            "_quality.overall": 1,
            "lineUp": 1
        }
    ).sort("dateTime.start", 1).limit(20))
    
    print(f"\n🏛️  EVENTS AT {venue_name.upper()}:")
    print("=" * 60)
    for event in events:
        print(f"📅 {event.get('dateTime', {}).get('displayText', 'N/A')}")
        print(f"   🎵 {event['title']}")
        print(f"   ⭐ Quality: {event['_quality']['overall']:.3f}")
        
        # Show headliners
        lineup = event.get('lineUp', [])
        headliners = [artist['name'] for artist in lineup if artist.get('headliner')]
        if headliners:
            print(f"   🎤 Headliners: {', '.join(headliners)}")
        print()
    
    client.close()
    return events


def get_events_with_issues():
    """Get events that have quality issues"""
    client = MongoClient()
    db = client.tickets_ibiza_events
    
    # Find events with validation flags
    pipeline = [
        {
            "$match": {
                "$or": [
                    {"_quality.overall": {"$lt": 0.7}},
                    {"_validation.title.flags": {"$ne": []}},
                    {"_validation.location.flags": {"$ne": []}},
                    {"_validation.dateTime.flags": {"$ne": []}}
                ]
            }
        },
        {
            "$project": {
                "title": 1,
                "url": 1,
                "_quality.overall": 1,
                "flags": {
                    "$concatArrays": [
                        "$_validation.title.flags",
                        "$_validation.location.flags",
                        "$_validation.dateTime.flags",
                        "$_validation.lineUp.flags",
                        "$_validation.ticketInfo.flags"
                    ]
                }
            }
        },
        {"$limit": 10}
    ]
    
    events = list(db.events.aggregate(pipeline))
    
    print("\n⚠️  EVENTS WITH QUALITY ISSUES:")
    print("=" * 60)
    for event in events:
        print(f"🎵 {event['title']}")
        print(f"   🔗 {event['url']}")
        print(f"   ⭐ Quality Score: {event['_quality']['overall']:.3f}")
        
        # Remove duplicates from flags
        flags = event.get('flags', [])
        if flags:
            unique_flags = list(set(flags))
            print(f"   🚩 Issues: {', '.join(unique_flags)}")
        print()
    
    client.close()
    return events


def get_quality_statistics():
    """Get overall quality statistics"""
    client = MongoClient()
    db = client.tickets_ibiza_events
    
    # Aggregate statistics
    pipeline = [
        {
            "$group": {
                "_id": None,
                "totalEvents": {"$sum": 1},
                "avgQuality": {"$avg": "$_quality.overall"},
                "minQuality": {"$min": "$_quality.overall"},
                "maxQuality": {"$max": "$_quality.overall"},
                "avgTitleScore": {"$avg": "$_quality.scores.title"},
                "avgLocationScore": {"$avg": "$_quality.scores.location"},
                "avgDateTimeScore": {"$avg": "$_quality.scores.dateTime"},
                "avgLineUpScore": {"$avg": "$_quality.scores.lineUp"},
                "avgTicketScore": {"$avg": "$_quality.scores.ticketInfo"}
            }
        }
    ]
    
    stats = list(db.events.aggregate(pipeline))[0]
    
    print("\n📊 QUALITY STATISTICS:")
    print("=" * 60)
    print(f"Total Events: {stats['totalEvents']}")
    print(f"\nOverall Quality:")
    print(f"  Average: {stats['avgQuality']:.3f}")
    print(f"  Minimum: {stats['minQuality']:.3f}")
    print(f"  Maximum: {stats['maxQuality']:.3f}")
    print(f"\nField Scores:")
    print(f"  Title:      {stats['avgTitleScore']:.3f}")
    print(f"  Location:   {stats['avgLocationScore']:.3f}")
    print(f"  DateTime:   {stats['avgDateTimeScore']:.3f}")
    print(f"  LineUp:     {stats['avgLineUpScore']:.3f}")
    print(f"  TicketInfo: {stats['avgTicketScore']:.3f}")
    
    # Get quality distribution
    distribution_pipeline = [
        {
            "$bucket": {
                "groupBy": "$_quality.overall",
                "boundaries": [0, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
                "default": "Other",
                "output": {
                    "count": {"$sum": 1},
                    "events": {"$push": "$title"}
                }
            }
        }
    ]
    
    distribution = list(db.events.aggregate(distribution_pipeline))
    
    print(f"\nQuality Distribution:")
    labels = ["Very Poor (0-0.5)", "Poor (0.5-0.6)", "Fair (0.6-0.7)", 
              "Good (0.7-0.8)", "Very Good (0.8-0.9)", "Excellent (0.9-1.0)"]
    
    for i, bucket in enumerate(distribution):
        if i < len(labels):
            print(f"  {labels[i]}: {bucket['count']} events")
    
    client.close()
    return stats


def search_events(search_term):
    """Search events by text"""
    client = MongoClient()
    db = client.tickets_ibiza_events
    
    # Text search on title and description
    events = list(db.events.find(
        {"$text": {"$search": search_term}},
        {
            "title": 1,
            "fullDescription": 1,
            "dateTime.displayText": 1,
            "location.venue": 1,
            "_quality.overall": 1,
            "score": {"$meta": "textScore"}
        }
    ).sort([("score", {"$meta": "textScore"})]).limit(10))
    
    print(f"\n🔍 SEARCH RESULTS FOR '{search_term}':")
    print("=" * 60)
    for event in events:
        print(f"🎵 {event['title']}")
        print(f"   📍 {event.get('location', {}).get('venue', 'Unknown')}")
        print(f"   📅 {event.get('dateTime', {}).get('displayText', 'N/A')}")
        print(f"   ⭐ Quality: {event['_quality']['overall']:.3f}")
        
        # Show snippet of description
        desc = event.get('fullDescription', '')
        if desc:
            snippet = desc[:150] + "..." if len(desc) > 150 else desc
            print(f"   📝 {snippet}")
        print()
    
    client.close()
    return events


def export_for_app(min_quality=0.7):
    """Export high-quality events in a format suitable for your app"""
    client = MongoClient()
    db = client.tickets_ibiza_events
    
    # Get events with good quality for app consumption
    events = list(db.events.find(
        {
            "_quality.overall": {"$gte": min_quality},
            "dateTime.start": {"$gte": datetime.utcnow()}  # Future events only
        },
        {
            "_id": 0,  # Exclude MongoDB ID
            "_quality": 0,  # Exclude internal quality data
            "_validation": 0  # Exclude internal validation data
        }
    ).sort("dateTime.start", 1).limit(100))
    
    # Save to JSON file
    with open("high_quality_events_export.json", "w", encoding="utf-8") as f:
        json.dump(events, f, indent=2, default=str)
    
    print(f"\n✅ Exported {len(events)} high-quality events to 'high_quality_events_export.json'")
    print(f"   Minimum quality score: {min_quality}")
    print(f"   Future events only: Yes")
    
    client.close()
    return events


def main():
    """Run example queries"""
    print("🎉 MongoDB Event Database Query Examples")
    print("=" * 80)
    
    # 1. Quality statistics
    get_quality_statistics()
    
    # 2. High quality events
    get_high_quality_events(0.8)
    
    # 3. Events by venue
    get_events_by_venue("Hï Ibiza")
    
    # 4. Events with issues
    get_events_with_issues()
    
    # 5. Search events
    search_events("techno")
    
    # 6. Export for app
    export_for_app(0.75)
    
    print("\n✅ All queries completed successfully!")


if __name__ == "__main__":
    main()