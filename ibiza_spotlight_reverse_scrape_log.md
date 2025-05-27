# Ibiza Spotlight Reverse Scrape Log

## Target Site Analysis

- **Target URL**: https://www.ibiza-spotlight.com/night/events
- **Site Type**: Event Calendar & Ticketing Platform
- **Analysis Date**: 2025-05-26, 1:43 PM (UTC+7)
- **Site Structure**: Calendar-based event listing with venue-specific pages

## Site Architecture Overview

### URL Structure Patterns
```
Base URL: https://www.ibiza-spotlight.com/
Calendar: /night/events/2025/{month}?daterange={start}-{end}
Monthly: /night/events/2025/{month} (e.g., /night/events/2025/07)
Event Detail: /night/events/{event-slug}
Venue Pages: /night/venues/{venue-slug}
```

### Navigation Flow
1. **Main Calendar Page** → Month Selection → **Event Listings** → **Individual Event Pages**
2. **Breadcrumb Pattern**: Home / Clubbing / Party calendar / Party guide

## Data Structure Analysis

### Event Listing Page Structure
```
Calendar View:
├── Date Range Selector (dropdown)
├── Month Navigation (JUN ← JULY → AUG)
├── Venue Sections (collapsible)
│   ├── [UNVRS] Events
│   ├── Amnesia Events  
│   ├── Other Venue Events
└── Individual Event Cards
```

### Individual Event Card Data
```
Event Card Contains:
├── Event Title (e.g., "Eric Prydz presents Holosphere 2.0")
├── Date & Time (e.g., "Mon 30 Jun", "23:30")
├── Venue Name (e.g., "[UNVRS]")
├── Artist Info (e.g., "Eric Prydz + more TBA")
├── Pricing Tiers:
│   ├── Early Entry (65€ before 01:00)
│   └── General Admission (85€ 2nd Release)
└── Action Button ("ADD TO BASKET")
```

### Event Detail Page Structure
```
Event Detail Page:
├── Hero Section
│   ├── Event Title
│   ├── Venue Name (clickable)
│   └── Description
├── Media Section (YouTube embed, images)
├── Event Listings Section
│   ├── Multiple Date Instances
│   ├── Pricing Information
│   └── Individual "ADD TO BASKET" buttons
└── BUY TICKETS (main CTA)
```

## Scraping Strategy for mono_ticketmaster.py

### Recommended Approach

#### 1. Entry Points
```python
# Primary entry points for crawling
ENTRY_URLS = [
    "https://www.ibiza-spotlight.com/night/events/2025/06",  # June
    "https://www.ibiza-spotlight.com/night/events/2025/07",  # July  
    "https://www.ibiza-spotlight.com/night/events/2025/08",  # August
    "https://www.ibiza-spotlight.com/night/events/2025/09",  # September
    "https://www.ibiza-spotlight.com/night/events/2025/10",  # October
]
```

#### 2. Event Link Extraction Selectors
```css
/* Calendar page event links */
.event-card a[href*="/night/events/"]
.event-listing a[href*="/night/events/"]

/* Event title links */
a[href*="/night/events/"]:has-text("presents")
```

#### 3. Data Extraction Selectors

##### Event Title
```css
h1:has-text("presents")
.event-title
h1[class*="title"]
```

##### Date & Time
```css
.event-date
.event-time
[class*="date"]
[class*="time"]
/* Pattern: "Mon 30 Jun" + "23:30" */
```

##### Venue Information
```css
/* Venue name */
a[href*="/night/venues/"]
.venue-name
[class*="venue"]

/* Venue in title pattern: "- [VENUE] -" */
```

##### Artist/Lineup
```css
.artist-name
.lineup
.performer
/* Pattern: "Artist Name + more TBA" */
```

##### Pricing Information
```css
.price
.ticket-price
[class*="price"]
/* Patterns: "65€", "85€" */
/* Text patterns: "Early Entry", "General Admission" */
```

##### Ticket URLs
```css
a[href*="ADD TO BASKET"]
.buy-tickets
.ticket-link
[class*="ticket"]
```

#### 4. JSON-LD Structured Data
The site may contain JSON-LD structured data. Check for:
```javascript
script[type="application/ld+json"]
// Look for @type: "MusicEvent" or "Event"
```

#### 5. Fallback Extraction Patterns

##### WordPress/Custom Selectors
```css
/* Title fallbacks */
h1.entry-title
.event-title
h1

/* Date fallbacks */
.event-date
.date
[data-date]

/* Venue fallbacks */
.venue
.location
[class*="venue"]

/* Price fallbacks */
.price
.cost
.ticket-price
```

##### Text Pattern Matching
```regex
# Date patterns
(\w{3}\s\d{1,2}\s\w{3})  # "Mon 30 Jun"
(\d{2}:\d{2})            # "23:30"

# Price patterns  
(\d+€)                   # "65€", "85€"
(Early Entry.*?€\d+)     # "Early Entry Ticket before 01:00"
(General Admission.*?€\d+) # "General Admission 2nd Release"

# Venue patterns
\[([A-Z]+)\]             # "[UNVRS]", "[VENUE]"
```

## Site-Specific Configuration

### Headers & User Agents
```python
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}
```

### Rate Limiting
```python
DELAYS = {
    "between_requests": (1.0, 2.5),  # Random delay between requests
    "between_pages": (2.0, 4.0),    # Longer delay between page crawls
}
```

### Browser Requirements
- **JavaScript Required**: Yes (for dynamic content loading)
- **Cookies**: Accept cookies banner present
- **Recommended**: Use Playwright for full page rendering

## Expected Data Quality

### High Confidence Fields
- Event Title (consistently structured)
- Date & Time (standardized format)
- Venue Name (clear venue identification)
- Pricing (clearly displayed with currency)

### Medium Confidence Fields  
- Artist Lineup (may include "TBA" elements)
- Event Description (varies by event)
- Ticket URLs (consistent button structure)

### Low Confidence Fields
- Event Categories/Genres (not explicitly shown)
- Age Restrictions (not visible in analysis)
- Detailed Venue Address (requires venue page crawl)

## Recommended Extraction Method Priority

1. **JSON-LD Structured Data** (if available)
2. **CSS Selectors** (primary method)
3. **Text Pattern Matching** (fallback)
4. **Meta Tags** (Open Graph, etc.)

## Pagination & Navigation

### Calendar Navigation
- Month-based pagination (JUN ← JULY → AUG)
- Date range filtering available
- Venue-based filtering (collapsible sections)

### Event Series Handling
- Events often have multiple dates (recurring series)
- Each date instance has separate pricing/availability
- Handle as individual events with series relationship

## Testing URLs

```
# Test URLs for validation
https://www.ibiza-spotlight.com/night/events/2025/07
https://www.ibiza-spotlight.com/night/events/eric-prydz-presents-holosphere-2-0-unvrs-2025

# Calendar with date range
https://www.ibiza-spotlight.com/night/events/2025/07?daterange=01/07/2025-31/07/2025
```

## Potential Challenges

1. **Dynamic Content**: Some content may load via JavaScript
2. **Cookie Consent**: Banner may interfere with scraping
3. **Rate Limiting**: Site may have anti-bot measures
4. **Seasonal Content**: Event availability varies by season
5. **Event Series**: Multiple instances of same event need deduplication logic

## Recommended mono_ticketmaster.py Modifications

### URL Patterns
```python
# Add to DEFAULT_TARGET_URL options
IBIZA_SPOTLIGHT_URLS = [
    "https://www.ibiza-spotlight.com/night/events/2025/06",
    "https://www.ibiza-spotlight.com/night/events/2025/07", 
    "https://www.ibiza-spotlight.com/night/events/2025/08",
]
```

### Custom Selectors
```python
# Add to extract_wordpress_data method
ibiza_selectors = {
    "title": ["h1:contains('presents')", ".event-title", "h1"],
    "date": [".event-date", ".date", "[data-date]"],
    "venue": ["a[href*='/night/venues/']", ".venue", "[class*='venue']"],
    "price": [".price", ".ticket-price", "[class*='price']"],
    "artist": [".artist", ".performer", ".lineup"],
}
```

### Event Link Extraction
```python
def extract_ibiza_event_links(soup):
    """Extract event links from Ibiza Spotlight calendar pages"""
    links = []
    
    # Event cards
    event_cards = soup.select("a[href*='/night/events/']")
    for card in event_cards:
        href = card.get('href')
        if href and 'presents' in card.get_text().lower():
            links.append(urljoin(base_url, href))
    
    return list(set(links))  # Remove duplicates
```

## Summary

Ibiza Spotlight is a well-structured event calendar site with consistent data patterns. The site uses a calendar-based navigation system with venue-specific sections. Events are clearly structured with standardized pricing displays and venue information. The site requires JavaScript for full functionality but has predictable URL patterns and CSS selectors that make it suitable for automated scraping with the mono_ticketmaster.py framework.

**Recommended Extraction Method**: Hybrid approach using CSS selectors as primary method with text pattern matching as fallback, utilizing Playwright for JavaScript-heavy pages.