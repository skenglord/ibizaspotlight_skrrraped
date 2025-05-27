#!/usr/bin/env python3
"""
Comprehensive test script for mono_ticketmaster.py
Tests all features and functions
"""

import json
import sys
from datetime import datetime
from pathlib import Path

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

from mono_ticketmaster import MultiLayerEventScraper, crawl_listing_for_events

def print_section(title):
    """Print a formatted section header"""
    print(f"\n{'='*80}")
    print(f"  {title}")
    print(f"{'='*80}\n")

def test_single_event_scraping():
    """Test scraping a single event"""
    print_section("TEST 1: Single Event Scraping")
    
    scraper = MultiLayerEventScraper(use_browser=False)
    url = "https://ticketsibiza.com/event/camelphat-pacha-2025-05-27/?wcs_timestamp=1748386740"
    
    print(f"Testing URL: {url}")
    data = scraper.scrape_event_data(url)
    
    if data:
        print("✓ Event data extracted successfully")
        
        # Test all fields
        print("\n1. Basic Information:")
        print(f"   - Title: {data.get('title', 'N/A')}")
        print(f"   - URL: {data.get('url', 'N/A')}")
        print(f"   - Extraction Method: {data.get('extractionMethod', 'N/A')}")
        print(f"   - Scraped At: {data.get('scrapedAt', 'N/A')}")
        
        print("\n2. Location Information:")
        location = data.get('location', {})
        print(f"   - Venue: {location.get('venue', 'N/A')}")
        print(f"   - Address: {location.get('address', 'N/A')}")
        coords = location.get('coordinates')
        if coords:
            print(f"   - Coordinates: {coords.get('lat')}, {coords.get('lng')}")
        
        print("\n3. Date/Time Information:")
        datetime_info = data.get('dateTime', {})
        print(f"   - Display Text: {datetime_info.get('displayText', 'N/A')}")
        print(f"   - Day of Week: {datetime_info.get('dayOfWeek', 'N/A')}")
        parsed = datetime_info.get('parsed', {})
        if parsed:
            print(f"   - Start Date: {parsed.get('startDate', 'N/A')}")
            print(f"   - End Date: {parsed.get('endDate', 'N/A')}")
            print(f"   - Doors: {parsed.get('doors', 'N/A')}")
        
        print("\n4. Lineup Information:")
        lineup = data.get('lineUp', [])
        print(f"   - Total Artists: {data.get('artistCount', 0)}")
        for i, artist in enumerate(lineup):
            headliner = " (Headliner)" if artist.get('headliner') else ""
            print(f"   - Artist {i+1}: {artist.get('name')}{headliner}")
            if artist.get('affiliates'):
                print(f"     Affiliates: {', '.join(artist['affiliates'])}")
            if artist.get('genres'):
                print(f"     Genres: {', '.join(artist['genres'])}")
        
        print("\n5. Ticket Information:")
        ticket_info = data.get('ticketInfo', {})
        if ticket_info:
            print(f"   - Display Text: {ticket_info.get('displayText', 'N/A')}")
            print(f"   - Starting Price: {ticket_info.get('startingPrice', 'N/A')} {ticket_info.get('currency', '')}")
            print(f"   - Status: {ticket_info.get('status', 'N/A')}")
            print(f"   - Ticket URL: {ticket_info.get('url', 'N/A')}")
            tiers = ticket_info.get('tiers', [])
            if tiers:
                print(f"   - Tiers: {len(tiers)} tier(s)")
                for tier in tiers:
                    print(f"     • {tier.get('name')}: {tier.get('price')} ({tier.get('available')})")
        
        print(f"   - Direct Ticket URL: {data.get('ticketsUrl', 'N/A')}")
        print(f"   - Has Ticket Info: {data.get('hasTicketInfo', False)}")
        print(f"   - Is Free: {data.get('isFree', False)}")
        print(f"   - Is Sold Out: {data.get('isSoldOut', False)}")
        
        print("\n6. Event Details:")
        print(f"   - Event Type: {', '.join(data.get('eventType', []))}")
        print(f"   - Genres: {', '.join(data.get('genres', []))}")
        print(f"   - Age Restriction: {data.get('ageRestriction', 'N/A')}")
        print(f"   - Promos: {', '.join(data.get('promos', []))}")
        
        print("\n7. Organizer Information:")
        organizer = data.get('organizer')
        if organizer:
            print(f"   - Name: {organizer.get('name', 'N/A')}")
            print(f"   - Affiliates: {', '.join(organizer.get('affiliates', []))}")
            social = organizer.get('socialLinks', {})
            if social:
                print(f"   - Social Links: {', '.join(social.keys())}")
        else:
            print("   - No organizer information")
        
        print("\n8. Media:")
        images = data.get('images', [])
        print(f"   - Images: {data.get('imageCount', 0)} image(s)")
        if images:
            for i, img in enumerate(images[:3]):  # Show first 3
                print(f"     • Image {i+1}: {img[:50]}...")
        
        social_links = data.get('socialLinks', {})
        if social_links:
            print(f"   - Event Social Links: {', '.join(social_links.keys())}")
        
        print("\n9. Description:")
        desc = data.get('fullDescription', '')
        if desc:
            print(f"   - Length: {len(desc)} characters")
            print(f"   - Preview: {desc[:150]}...")
        else:
            print("   - No description")
        
        return True
    else:
        print("✗ Failed to extract event data")
        return False

def test_strategic_scraping():
    """Test strategic scraping (requests first, then browser if needed)"""
    print_section("TEST 2: Strategic Scraping")
    
    scraper = MultiLayerEventScraper(use_browser=True, headless=True)
    url = "https://ticketsibiza.com/event/camelphat-pacha-2025-05-27/?wcs_timestamp=1748386740"
    
    print(f"Testing strategic scraping for: {url}")
    data = scraper.scrape_event_strategically(url)
    
    if data:
        print(f"✓ Strategic scraping successful")
        print(f"  - Extraction method used: {data.get('extractionMethod')}")
        print(f"  - Title: {data.get('title')}")
        print(f"  - Artists found: {data.get('artistCount', 0)}")
        return True
    else:
        print("✗ Strategic scraping failed")
        return False

def test_user_agent_rotation():
    """Test User-Agent rotation"""
    print_section("TEST 3: User-Agent Rotation")
    
    scraper = MultiLayerEventScraper(use_browser=False)
    
    print(f"Initial User-Agent: {scraper.current_user_agent}")
    print(f"Rotation scheduled after: {scraper.rotate_ua_after_pages} pages")
    
    # Force rotation
    original_ua = scraper.current_user_agent
    scraper.rotate_user_agent()
    new_ua = scraper.current_user_agent
    
    print(f"After rotation: {new_ua}")
    print(f"✓ User-Agent changed: {original_ua != new_ua}")
    
    return original_ua != new_ua

def test_fallback_extraction():
    """Test fallback extraction when JSON-LD is not available"""
    print_section("TEST 4: Fallback Extraction")
    
    scraper = MultiLayerEventScraper(use_browser=False)
    
    # Test with a page that might not have JSON-LD
    # Using the listing page as it typically doesn't have event JSON-LD
    url = "https://ticketsibiza.com/ibiza-calendar/2025-events/"
    
    print(f"Testing fallback extraction on: {url}")
    data = scraper.scrape_event_data(url)
    
    if data:
        print(f"✓ Fallback extraction successful")
        print(f"  - Extraction method: {data.get('extractionMethod')}")
        print(f"  - Title: {data.get('title', 'N/A')}")
        print(f"  - Extracted data keys: {list(data.get('extractedData', {}).keys())}")
        return True
    else:
        print("✗ Fallback extraction failed")
        return False

def test_crawl_listing():
    """Test crawling a listing page for events"""
    print_section("TEST 5: Listing Page Crawling")
    
    # Check if playwright is available
    try:
        from playwright.sync_api import sync_playwright
        playwright_available = True
    except ImportError:
        playwright_available = False
        print("⚠ Playwright not installed - skipping crawl test")
        return None
    
    if playwright_available:
        scraper = MultiLayerEventScraper(use_browser=True, headless=True)
        listing_url = "https://ticketsibiza.com/ibiza-calendar/2025-events/"
        
        print(f"Testing crawl on: {listing_url}")
        print("(Limiting to 3 events for test)")
        
        events = crawl_listing_for_events(
            listing_url, 
            scraper, 
            max_pages=3,
            headless=True
        )
        
        if events:
            print(f"✓ Crawled {len(events)} events successfully")
            for i, event in enumerate(events):
                print(f"  {i+1}. {event.get('title', 'Unknown')} - {event.get('extractionMethod')}")
            return True
        else:
            print("✗ No events crawled")
            return False

def test_data_validation():
    """Test data validation and derived fields"""
    print_section("TEST 6: Data Validation & Derived Fields")
    
    scraper = MultiLayerEventScraper(use_browser=False)
    url = "https://ticketsibiza.com/event/camelphat-pacha-2025-05-27/?wcs_timestamp=1748386740"
    
    data = scraper.scrape_event_data(url)
    
    if data:
        print("✓ Checking derived fields:")
        
        # Check if derived fields are populated
        checks = [
            ("artistCount matches lineUp length", 
             data.get('artistCount') == len(data.get('lineUp', []))),
            ("imageCount matches images length", 
             data.get('imageCount') == len(data.get('images', []))),
            ("updatedAt is set", 
             data.get('updatedAt') is not None),
            ("lastCheckedAt is set", 
             data.get('lastCheckedAt') is not None),
            ("hasTicketInfo is properly set", 
             data.get('hasTicketInfo') == bool(data.get('ticketInfo'))),
        ]
        
        all_passed = True
        for check_name, result in checks:
            status = "✓" if result else "✗"
            print(f"  {status} {check_name}")
            if not result:
                all_passed = False
        
        return all_passed
    else:
        print("✗ No data to validate")
        return False

def test_output_formats():
    """Test JSON and Markdown output formats"""
    print_section("TEST 7: Output Formats")
    
    scraper = MultiLayerEventScraper(use_browser=False)
    url = "https://ticketsibiza.com/event/camelphat-pacha-2025-05-27/?wcs_timestamp=1748386740"
    
    data = scraper.scrape_event_data(url)
    
    if data:
        # Test JSON serialization
        try:
            from mono_ticketmaster import datetime_serializer
            json_str = json.dumps(data, indent=2, default=datetime_serializer)
            print(f"✓ JSON serialization successful ({len(json_str)} characters)")
            
            # Test Markdown formatting
            from mono_ticketmaster import format_event_to_markdown
            markdown = format_event_to_markdown(data)
            print(f"✓ Markdown formatting successful ({len(markdown)} characters)")
            
            # Check if markdown contains key sections
            md_checks = [
                ("Title section", "##" in markdown),
                ("Location section", "### Location" in markdown),
                ("Lineup section", "### Lineup" in markdown),
                ("Ticket section", "### Ticket" in markdown),
                ("Direct ticket link", "Buy Tickets:" in markdown),
            ]
            
            for check_name, result in md_checks:
                status = "✓" if result else "✗"
                print(f"  {status} Markdown contains {check_name}")
            
            return True
        except Exception as e:
            print(f"✗ Output format error: {e}")
            return False
    else:
        print("✗ No data to format")
        return False

def main():
    """Run all tests"""
    print("\n" + "="*80)
    print("  MONO_TICKETMASTER.PY COMPREHENSIVE TEST SUITE")
    print("="*80)
    
    tests = [
        ("Single Event Scraping", test_single_event_scraping),
        ("Strategic Scraping", test_strategic_scraping),
        ("User-Agent Rotation", test_user_agent_rotation),
        ("Fallback Extraction", test_fallback_extraction),
        ("Listing Page Crawling", test_crawl_listing),
        ("Data Validation", test_data_validation),
        ("Output Formats", test_output_formats),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"\n✗ Test '{test_name}' failed with error: {e}")
            import traceback
            traceback.print_exc()
            results.append((test_name, False))
    
    # Summary
    print_section("TEST SUMMARY")
    
    passed = sum(1 for _, result in results if result is True)
    failed = sum(1 for _, result in results if result is False)
    skipped = sum(1 for _, result in results if result is None)
    total = len(results)
    
    print(f"Total Tests: {total}")
    print(f"✓ Passed: {passed}")
    print(f"✗ Failed: {failed}")
    print(f"⚠ Skipped: {skipped}")
    print(f"\nSuccess Rate: {(passed/total)*100:.1f}%")
    
    print("\nDetailed Results:")
    for test_name, result in results:
        if result is True:
            status = "✓ PASS"
        elif result is False:
            status = "✗ FAIL"
        else:
            status = "⚠ SKIP"
        print(f"  {status} - {test_name}")

if __name__ == "__main__":
    main()