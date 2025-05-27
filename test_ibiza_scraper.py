#!/usr/bin/env python3

import json
import sys
from datetime import datetime
from pathlib import Path

# Add the current directory to sys.path
sys.path.insert(0, str(Path(__file__).parent))

from mono_ibiza_spotlight import MultiLayerEventScraper, datetime_serializer

def test_scraper_with_mock_data():
    """Test the scraper with mock event data to demonstrate functionality"""
    
    # Create mock event data based on our analysis
    mock_events = [
        {
            "url": "https://www.ibiza-spotlight.com/night/events/eric-prydz-holosphere-mock-1",
            "title": "Eric Prydz presents Holosphere 2.0",
            "venue": "[UNVRS]",
            "date": "Mon 30 Jun",
            "time": "23:30",
            "price_early": "65€",
            "price_general": "85€",
            "description": "At [UNVRS]. Ground-breaking visuals and immersive theatre from production specialist Eric Prydz."
        },
        {
            "url": "https://www.ibiza-spotlight.com/night/events/amnesia-presents-mock-2", 
            "title": "Amnesia Presents",
            "venue": "Amnesia",
            "date": "Tue 01 Jul",
            "time": "00:00",
            "price_early": "45€",
            "price_general": "65€",
            "description": "The legendary Amnesia experience continues with world-class DJs."
        },
        {
            "url": "https://www.ibiza-spotlight.com/night/events/do-not-sleep-mock-3",
            "title": "Do Not Sleep",
            "venue": "Cova Santa",
            "date": "Wed 02 Jul", 
            "time": "22:00",
            "price_early": "40€",
            "price_general": "55€",
            "description": "Underground house and techno in the mystical setting of Cova Santa."
        },
        {
            "url": "https://www.ibiza-spotlight.com/night/events/pacha-opening-mock-4",
            "title": "Pacha Opening Party",
            "venue": "Pacha",
            "date": "Thu 03 Jul",
            "time": "23:00", 
            "price_early": "70€",
            "price_general": "90€",
            "description": "The iconic Pacha opens its doors for another legendary season."
        },
        {
            "url": "https://www.ibiza-spotlight.com/night/events/ushuaia-closing-mock-5",
            "title": "Ushuaïa Closing Festival",
            "venue": "Ushuaïa",
            "date": "Fri 04 Jul",
            "time": "16:00",
            "price_early": "80€", 
            "price_general": "100€",
            "description": "The ultimate poolside party experience as Ushuaïa closes the season."
        }
    ]
    
    print("=== Testing Ibiza Spotlight Scraper with Mock Data ===\n")
    
    scraper = MultiLayerEventScraper(use_browser=False)  # Use requests only for mock data
    processed_events = []
    
    for i, mock_event in enumerate(mock_events, 1):
        print(f"Processing mock event {i}/5: {mock_event['title']}")
        
        # Convert mock data to our schema format
        event_data = scraper._map_fallback_to_event_schema(
            data={
                "title": mock_event["title"],
                "venue": mock_event["venue"],
                "date_text": f"{mock_event['date']} {mock_event['time']}",
                "price_text": f"Early Entry {mock_event['price_early']}, General Admission {mock_event['price_general']}",
                "description": mock_event["description"]
            },
            url=mock_event["url"],
            html=f"<html><body><h1>{mock_event['title']}</h1><p>{mock_event['description']}</p></body></html>",
            now_iso=datetime.utcnow().isoformat() + "Z",
            soup=None
        )
        
        processed_events.append(event_data)
        
        # Print summary
        print(f"  ✓ Title: {event_data.get('title')}")
        print(f"  ✓ Venue: {event_data.get('location', {}).get('venue')}")
        print(f"  ✓ Date: {event_data.get('dateTime', {}).get('displayText')}")
        
        ticket_info = event_data.get('ticketInfo')
        if ticket_info and ticket_info.get('tiers'):
            prices = [f"{tier['name']}: {tier['price']}€" for tier in ticket_info['tiers']]
            print(f"  ✓ Pricing: {', '.join(prices)}")
        
        print(f"  ✓ Has Ticket Info: {event_data.get('hasTicketInfo')}")
        print()
    
    # Save results
    output_dir = Path("output/ibiza_spotlight")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save JSON
    json_path = output_dir / f"mock_events_{timestamp}.json"
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(processed_events, f, indent=2, ensure_ascii=False, default=datetime_serializer)
    
    print(f"✅ Saved {len(processed_events)} mock events to: {json_path}")
    
    # Create summary
    print("\n=== Event Summary ===")
    for idx, event in enumerate(processed_events, 1):
        title = event.get("title", "Unknown")
        venue = event.get("location", {}).get("venue", "Unknown")
        method = event.get("extractionMethod", "unknown")
        has_tickets = "✓" if event.get("hasTicketInfo") else "✗"
        print(f"{idx}. {title} @ {venue} (via {method}) [Tickets: {has_tickets}]")
    
    # Test data quality
    print(f"\n=== Data Quality Assessment ===")
    total_events = len(processed_events)
    events_with_titles = sum(1 for e in processed_events if e.get("title"))
    events_with_venues = sum(1 for e in processed_events if e.get("location", {}).get("venue"))
    events_with_dates = sum(1 for e in processed_events if e.get("dateTime", {}).get("displayText"))
    events_with_tickets = sum(1 for e in processed_events if e.get("hasTicketInfo"))
    
    print(f"Events with titles: {events_with_titles}/{total_events} ({events_with_titles/total_events*100:.1f}%)")
    print(f"Events with venues: {events_with_venues}/{total_events} ({events_with_venues/total_events*100:.1f}%)")
    print(f"Events with dates: {events_with_dates}/{total_events} ({events_with_dates/total_events*100:.1f}%)")
    print(f"Events with ticket info: {events_with_tickets}/{total_events} ({events_with_tickets/total_events*100:.1f}%)")
    
    return processed_events

if __name__ == "__main__":
    test_scraper_with_mock_data()