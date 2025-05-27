import argparse
from datetime import datetime
import json
import random
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, TypedDict
from urllib.parse import urljoin, urlparse

# Add the current directory to sys.path to fix import issues
sys.path.insert(0, str(Path(__file__).parent))

# Import the specific module directly without going through __init__.py
import importlib.util
spec = importlib.util.spec_from_file_location("convert_to_md",
                                               Path(__file__).parent / "utils" / "convert_to_md.py")
convert_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(convert_module)
convert_to_md = convert_module.convert_to_md
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from playwright.sync_api import sync_playwright
except Exception:  # pragma: no cover - playwright may not be installed
    sync_playwright = None

DEFAULT_TARGET_URL = "https://www.ibiza-spotlight.com/night/events/2025/05?daterange=26/05/2025-01/06/2025"

MODERN_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0",
]

# Type Definitions for Event Schema

class CoordinatesTypedDict(TypedDict, total=False):
    lat: Optional[float]
    lng: Optional[float]

class LocationTypedDict(TypedDict, total=False):
    venue: Optional[str]
    address: Optional[str]
    coordinates: Optional[CoordinatesTypedDict]

class ParsedDateTimeTypedDict(TypedDict, total=False):
    startDate: Optional[datetime]
    endDate: Optional[datetime]
    doors: Optional[str]

class DateTimeInfoTypedDict(TypedDict, total=False):
    displayText: Optional[str]
    parsed: Optional[ParsedDateTimeTypedDict]
    dayOfWeek: Optional[str]

class ArtistTypedDict(TypedDict, total=False):
    name: str
    affiliates: Optional[List[str]]
    genres: Optional[List[str]]
    headliner: Optional[bool]

class TicketTierTypedDict(TypedDict, total=False):
    name: Optional[str]
    price: Optional[float]
    available: Optional[bool]

class TicketInfoTypedDict(TypedDict, total=False):
    displayText: Optional[str]
    startingPrice: Optional[float]
    currency: Optional[str]
    tiers: Optional[List[TicketTierTypedDict]]
    status: Optional[str]
    url: Optional[str]

class OrganizerTypedDict(TypedDict, total=False):
    name: Optional[str]
    affiliates: Optional[List[str]]
    socialLinks: Optional[Dict[str, str]]

class EventSchemaTypedDict(TypedDict, total=False):
    title: Optional[str]
    url: str
    location: Optional[LocationTypedDict]
    dateTime: Optional[DateTimeInfoTypedDict]
    lineUp: Optional[List[ArtistTypedDict]]
    eventType: Optional[List[str]]
    genres: Optional[List[str]]
    ticketInfo: Optional[TicketInfoTypedDict]
    promos: Optional[List[str]]
    organizer: Optional[OrganizerTypedDict]
    ageRestriction: Optional[str]
    images: Optional[List[str]]
    socialLinks: Optional[Dict[str, str]]
    fullDescription: Optional[str]
    hasTicketInfo: Optional[bool]
    isFree: Optional[bool]
    isSoldOut: Optional[bool]
    artistCount: Optional[int]
    imageCount: Optional[int]
    scrapedAt: datetime
    updatedAt: Optional[datetime]
    lastCheckedAt: Optional[datetime]
    extractionMethod: Optional[str]
    html: Optional[str]
    extractedData: Optional[Dict]
    ticketsUrl: Optional[str]

# Data validation functions
def validate_price(price_str: str) -> Optional[float]:
    """Extract and validate price from string."""
    if not price_str:
        return None
    
    # Remove non-numeric characters except decimal points
    price_clean = re.sub(r'[^\d.,]', '', price_str)
    
    # Handle different decimal separators
    price_clean = price_clean.replace(',', '.')
    
    # Extract numeric value
    price_match = re.search(r'(\d+(?:\.\d{1,2})?)', price_clean)
    if price_match:
        try:
            price = float(price_match.group(1))
            # Validate reasonable price range (€5 to €500)
            if 5 <= price <= 500:
                return price
            # If price seems too high, it might be a parsing error
            elif price > 1000:
                # Try to extract a more reasonable price
                smaller_prices = re.findall(r'\b(\d{1,3})\b', price_str)
                for p in smaller_prices:
                    p_val = float(p)
                    if 5 <= p_val <= 500:
                        return p_val
        except ValueError:
            pass
    
    return None

def clean_artist_name(name: str) -> Optional[str]:
    """Clean and validate artist name."""
    if not name or len(name.strip()) < 2:
        return None
    
    # Remove excessive whitespace and newlines
    cleaned = re.sub(r'\s+', ' ', name.strip())
    
    # Remove date/time patterns that got mixed in
    cleaned = re.sub(r'\b(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\b.*', '', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\b\d{1,2}:\d{2}\b.*', '', cleaned)
    cleaned = re.sub(r'\bfrom\s+\d{2}:\d{2}.*', '', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\b\d{1,2}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b.*', '', cleaned, flags=re.IGNORECASE)
    
    # Remove ticket/price information
    cleaned = re.sub(r'€\d+.*', '', cleaned)
    cleaned = re.sub(r'\b(General Admission|Early Entry|Ticket).*', '', cleaned, flags=re.IGNORECASE)
    
    # Remove venue information that got mixed in
    cleaned = re.sub(r'\b(Las Dalias|Akasha|Eden|Pacha|Amnesia)\b.*', '', cleaned, flags=re.IGNORECASE)
    
    # Final cleanup
    cleaned = cleaned.strip()
    
    # Validate length and content
    if len(cleaned) < 2 or len(cleaned) > 100:
        return None
    
    # Check if it's mostly alphabetic (allow some special characters)
    if not re.search(r'[a-zA-Z]', cleaned):
        return None
    
    return cleaned

def parse_date_text(date_text: str) -> Optional[datetime]:
    """Parse various date formats."""
    if not date_text:
        return None
    
    # Common date patterns
    patterns = [
        r'(\d{1,2})/(\d{1,2})/(\d{4})',  # DD/MM/YYYY
        r'(\d{4})-(\d{1,2})-(\d{1,2})',  # YYYY-MM-DD
        r'(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{4})',  # DD MMM YYYY
    ]
    
    months = {
        'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
        'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
    }
    
    for pattern in patterns:
        match = re.search(pattern, date_text, re.IGNORECASE)
        if match:
            try:
                if len(match.groups()) == 3:
                    if pattern.endswith(r'(\d{4})'):  # DD/MM/YYYY or DD MMM YYYY
                        if match.group(2).isdigit():  # DD/MM/YYYY
                            day, month, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
                        else:  # DD MMM YYYY
                            day, month_str, year = int(match.group(1)), match.group(2), int(match.group(3))
                            month = months.get(month_str.capitalize(), 1)
                    else:  # YYYY-MM-DD
                        year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
                    
                    return datetime(year, month, day)
            except (ValueError, TypeError):
                continue
    
    return None

def is_data_sufficient(event_data: Dict) -> bool:
    """Checks if the extracted event data is sufficient with improved validation."""
    if not event_data:
        return False
    
    # Check if JSON-LD data was found and has a title
    if event_data.get("extractionMethod") == "jsonld" and event_data.get("title"):
        return True
    
    # Check if fallback data has a title and at least one other key piece of info
    if event_data.get("extractionMethod") == "fallback":
        title = event_data.get("title", "")
        venue = event_data.get("location", {}).get("venue", "")
        date_info = event_data.get("dateTime", {}).get("displayText", "")
        price_info = event_data.get("ticketInfo", {}).get("startingPrice", 0)
        description = event_data.get("fullDescription", "")
        
        # More stringent validation
        has_title = title and len(title.strip()) > 10 and "cookie" not in title.lower()
        has_venue = venue and len(venue.strip()) > 2
        has_date = date_info and len(date_info.strip()) > 5
        has_price = price_info and price_info > 0
        has_description = description and len(description.strip()) > 20 and "cookie" not in description.lower()
        
        if has_title and (has_venue or has_date or has_price or has_description):
            return True
    
    return False


class ImprovedMultiLayerEventScraper:
    def __init__(
        self,
        use_browser: bool = True,
        headless: bool = True,
        playwright_slow_mo: int = 62,
        random_delay_range: tuple = (1.0, 2.5),
        user_agents: Optional[List[str]] = None,
    ):
        self.use_browser = use_browser and sync_playwright is not None
        self.headless = headless
        self.playwright_slow_mo = playwright_slow_mo
        self.random_delay_range = random_delay_range
        self.user_agents = user_agents or MODERN_USER_AGENTS
        self.current_user_agent: Optional[str] = None
        self.pages_scraped_since_ua_rotation: int = 0
        self.rotate_ua_after_pages: int = random.randint(6, 12)
        self.rotate_user_agent()

    def rotate_user_agent(self):
        """Rotates the User-Agent and re-initializes the session."""
        self.current_user_agent = random.choice(self.user_agents)
        self.session = self._setup_session()
        self.pages_scraped_since_ua_rotation = 0
        self.rotate_ua_after_pages = random.randint(6, 12)

    def _setup_session(self):
        """Setup HTTP session with retries and browser-like headers."""
        session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        headers = {
            "User-Agent": self.current_user_agent or MODERN_USER_AGENTS[0],
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
        }
        session.headers.update(headers)
        return session

    def fetch_page(self, url: str, use_browser_for_this_fetch: bool = False) -> Optional[str]:
        """Fetch page HTML with error handling and strategic browser use."""
        if self.use_browser and use_browser_for_this_fetch and sync_playwright is not None:
            try:
                with sync_playwright() as p:
                    browser = p.chromium.launch(
                        headless=self.headless, slow_mo=self.playwright_slow_mo
                    )
                    context = browser.new_context(
                        user_agent=self.current_user_agent,
                        viewport={'width': 1920, 'height': 1080}
                    )
                    page = context.new_page()
                    
                    # Handle cookie consent
                    page.goto(url, timeout=30000)
                    
                    # Try to accept cookies if banner appears
                    try:
                        cookie_button = page.locator('text="NO PROBLEM"').first
                        if cookie_button.is_visible(timeout=3000):
                            cookie_button.click()
                            time.sleep(1)
                    except:
                        pass  # Cookie banner might not appear
                    
                    # Wait for content to load
                    page.wait_for_load_state('networkidle')
                    content = page.content()
                    browser.close()
                    return content
            except Exception as e:
                print(f"Browser fetch failed for {url}: {e}", file=sys.stderr)
                return None
        else:
            # Fallback to requests
            time.sleep(random.uniform(self.random_delay_range[0], self.random_delay_range[1]))
            try:
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
                return response.text
            except Exception as e:
                print(f"Error fetching {url} with requests: {e}", file=sys.stderr)
                return None

    def extract_improved_price_data(self, html: str) -> Dict:
        """Improved price extraction with better validation."""
        data = {}
        
        # Enhanced price patterns
        price_patterns = [
            r'(\d{1,3})€',  # Simple price like "65€"
            r'€(\d{1,3}(?:\.\d{2})?)',  # Euro symbol first
            r'(\d{1,3}(?:\.\d{2})?)\s*€',  # Price with euro symbol
            r'(General Admission[^€]*€\s*(\d{1,3}))',  # General admission with price
            r'(Early Entry[^€]*€\s*(\d{1,3}))',  # Early entry with price
        ]
        
        prices_found = []
        for pattern in price_patterns:
            matches = re.finditer(pattern, html, re.IGNORECASE)
            for match in matches:
                # Extract the numeric part
                price_text = match.group(0)
                validated_price = validate_price(price_text)
                if validated_price:
                    prices_found.append({
                        'price': validated_price,
                        'text': price_text,
                        'context': match.group(0)
                    })
        
        if prices_found:
            # Sort by price and take the lowest reasonable one
            prices_found.sort(key=lambda x: x['price'])
            data['validated_price'] = prices_found[0]['price']
            data['price_text'] = prices_found[0]['text']
            data['all_prices'] = [p['price'] for p in prices_found]
        
        return data

    def extract_improved_artist_data(self, soup: BeautifulSoup) -> List[str]:
        """Improved artist extraction with better cleaning."""
        artists = []
        
        # Look for artist mentions in various contexts
        text = soup.get_text()
        
        # Enhanced artist patterns
        artist_patterns = [
            r'([A-Za-z][A-Za-z\s&\.]{2,30})\s*\+\s*more\s*TBA',  # "Artist + more TBA"
            r'presents\s+([A-Za-z][A-Za-z\s&\.]{2,30})(?=\s*[-\n])',  # After "presents"
            r'lineup[:\s]*([A-Za-z][A-Za-z\s&\.]{2,30})',  # After "lineup"
        ]
        
        for pattern in artist_patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                artist_name = clean_artist_name(match.group(1))
                if artist_name and artist_name not in artists:
                    artists.append(artist_name)
        
        # Look for specific artist elements in HTML
        artist_selectors = [
            '.artist-name',
            '.performer',
            '.lineup-item',
            '[class*="artist"]'
        ]
        
        for selector in artist_selectors:
            try:
                elements = soup.select(selector)
                for elem in elements:
                    artist_name = clean_artist_name(elem.get_text())
                    if artist_name and artist_name not in artists:
                        artists.append(artist_name)
            except:
                continue
        
        # Remove duplicates while preserving order
        unique_artists = []
        seen = set()
        for artist in artists:
            if artist not in seen:
                seen.add(artist)
                unique_artists.append(artist)
        
        return unique_artists[:10]  # Limit to reasonable number

    def extract_improved_date_data(self, html: str) -> Dict:
        """Improved date extraction with better parsing."""
        data = {}
        
        # Enhanced date patterns
        date_patterns = [
            r'(\w{3}\s+\d{1,2}\s+\w{3})',  # "Mon 30 May"
            r'(\d{1,2}/\d{1,2}/\d{4})',    # "30/05/2025"
            r'(\d{4}-\d{2}-\d{2})',        # "2025-05-30"
            r'(\d{1,2}\s+\w+\s+\d{4})',   # "30 May 2025"
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, html)
            if match:
                date_text = match.group(0)
                parsed_date = parse_date_text(date_text)
                if parsed_date:
                    data['date_text'] = date_text
                    data['parsed_date'] = parsed_date
                    data['day_of_week'] = parsed_date.strftime('%A')
                    break
        
        # Time patterns
        time_patterns = [
            r'(\d{1,2}:\d{2})',
            r'from\s+(\d{1,2}:\d{2})',
        ]
        
        for pattern in time_patterns:
            match = re.search(pattern, html)
            if match:
                data['time_text'] = match.group(1)
                break
        
        return data

    def scrape_event_data_improved(self, url: str, attempt_with_browser: bool = False) -> Dict:
        """Main scraping method with improved extraction strategies."""
        html = self.fetch_page(url, use_browser_for_this_fetch=attempt_with_browser)
        if not html:
            return {}

        soup = BeautifulSoup(html, "html.parser")
        now_iso = datetime.utcnow().isoformat() + "Z"

        # Try JSON-LD first (existing logic)
        jsonld_data = self.extract_jsonld_data(soup)
        if jsonld_data:
            return self._map_jsonld_to_event_schema(jsonld_data, url, html, now_iso)

        # Enhanced fallback extraction
        price_data = self.extract_improved_price_data(html)
        artist_data = self.extract_improved_artist_data(soup)
        date_data = self.extract_improved_date_data(html)
        
        # Basic extraction (existing methods)
        basic_data = self.extract_ibiza_spotlight_data(soup)
        meta_data = self.extract_meta_data(soup)
        
        # Combine all data
        combined_data = {**basic_data, **meta_data, **price_data, **date_data}
        combined_data['extracted_artists'] = artist_data
        
        return self._map_improved_fallback_to_event_schema(combined_data, url, html, now_iso, soup)

    def _map_improved_fallback_to_event_schema(
        self, data: Dict, url: str, html: str, now_iso: str, soup: BeautifulSoup = None
    ) -> EventSchemaTypedDict:
        """Build schema from improved fallback extraction methods."""
        
        scraped_at_datetime = datetime.utcnow()
        try:
            scraped_at_datetime = datetime.fromisoformat(now_iso.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            pass

        # Initialize with improved validation
        event_data: EventSchemaTypedDict = {
            "url": url,
            "scrapedAt": scraped_at_datetime,
            "extractionMethod": "improved_fallback",
            "html": html[:5000] if html else None,
            "title": None,
            "location": None,
            "dateTime": None,
            "lineUp": [],
            "eventType": [],
            "genres": [],
            "ticketInfo": None,
            "promos": [],
            "organizer": None,
            "ageRestriction": None,
            "images": [],
            "socialLinks": {},
            "fullDescription": None,
            "hasTicketInfo": False,
            "isFree": False,
            "isSoldOut": False,
            "artistCount": None,
            "imageCount": None,
            "updatedAt": None,
            "lastCheckedAt": None,
            "extractedData": data,
            "ticketsUrl": None
        }

        # Improved title extraction
        title = data.get("title", "")
        if title and "cookie" not in title.lower() and len(title.strip()) > 10:
            event_data["title"] = title.strip()

        # Improved location
        venue = data.get("venue") or data.get("venue_pattern")
        if venue and len(venue.strip()) > 2:
            event_data["location"] = {
                "venue": venue.strip(),
                "address": None,
                "coordinates": None,
            }

        # Improved date/time
        date_text = data.get("date_text", "")
        parsed_date = data.get("parsed_date")
        time_text = data.get("time_text", "")
        
        if date_text or time_text or parsed_date:
            display_parts = []
            if date_text:
                display_parts.append(date_text)
            if time_text:
                display_parts.append(time_text)
            
            event_data["dateTime"] = {
                "displayText": " ".join(display_parts) if display_parts else None,
                "parsed": {
                    "startDate": parsed_date,
                    "endDate": None,
                    "doors": time_text if time_text else None,
                },
                "dayOfWeek": data.get("day_of_week"),
            }

        # Improved lineup
        artists = data.get("extracted_artists", [])
        lineup_list = []
        for idx, artist_name in enumerate(artists):
            if artist_name:
                lineup_list.append({
                    "name": artist_name,
                    "affiliates": [],
                    "genres": [],
                    "headliner": idx == 0,
                })
        event_data["lineUp"] = lineup_list

        # Improved ticket info
        validated_price = data.get("validated_price")
        price_text = data.get("price_text")
        
        if validated_price or price_text:
            event_data["ticketInfo"] = {
                "displayText": price_text,
                "startingPrice": validated_price,
                "currency": "EUR",
                "tiers": [],
                "status": None,
                "url": None,
            }

        # Improved description
        description = data.get("description", "")
        if description and "cookie" not in description.lower() and len(description.strip()) > 20:
            event_data["fullDescription"] = description.strip()

        # Populate derived fields
        self._populate_derived_fields(event_data)
        
        return event_data

    # Include existing methods (extract_jsonld_data, extract_ibiza_spotlight_data, etc.)
    # ... (copy from original script)

    def extract_jsonld_data(self, soup: BeautifulSoup) -> Optional[Dict]:
        """Extract JSON-LD structured data."""
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                raw_ld = script.string or script.get_text()
                data_ld = json.loads(raw_ld)

                graph = data_ld.get("@graph", []) if isinstance(data_ld, dict) else []
                for node in graph:
                    if node.get("@type") in ["MusicEvent", "Event"]:
                        return node

                if isinstance(data_ld, dict) and data_ld.get("@type") in ["MusicEvent", "Event"]:
                    return data_ld
            except Exception:
                continue
        return None

    def extract_ibiza_spotlight_data(self, soup: BeautifulSoup) -> Dict:
        """Extract data using Ibiza Spotlight specific selectors."""
        data: Dict[str, str] = {}

        # Title extraction
        title_selectors = [
            "h1:contains('presents')",
            ".event-title", 
            "h1",
            ".entry-title"
        ]
        for selector in title_selectors:
            try:
                title_elem = soup.select_one(selector)
                if title_elem:
                    data["title"] = title_elem.get_text(strip=True)
                    break
            except:
                continue

        # Venue extraction
        venue_selectors = [
            "a[href*='/night/venues/']",
            ".venue",
            ".venue-name",
            '[class*="venue"]'
        ]
        for selector in venue_selectors:
            try:
                venue_elem = soup.select_one(selector)
                if venue_elem:
                    data["venue"] = venue_elem.get_text(strip=True)
                    break
            except:
                continue

        # Description extraction
        desc_selectors = [
            ".event-description",
            ".description",
            ".entry-content",
            "p"
        ]
        for selector in desc_selectors:
            try:
                desc_elem = soup.select_one(selector)
                if desc_elem:
                    data["description"] = desc_elem.get_text(strip=True)[:500]
                    break
            except:
                continue

        return data

    def extract_meta_data(self, soup: BeautifulSoup) -> Dict:
        """Extract Open Graph and meta tag data."""
        data: Dict[str, str] = {}

        og_mappings = {
            "og:title": "title",
            "og:description": "description",
            "og:image": "image",
            "og:url": "canonical_url",
        }
        for og_prop, key in og_mappings.items():
            meta = soup.find("meta", property=og_prop)
            if meta and meta.get("content"):
                data[key] = meta["content"]

        meta_mappings = {
            "description": "meta_description",
            "keywords": "keywords",
        }
        for name, key in meta_mappings.items():
            meta = soup.find("meta", attrs={"name": name})
            if meta and meta.get("content"):
                data[key] = meta["content"]

        return data

    def _map_jsonld_to_event_schema(
        self, node: Dict, url: str, html: str, now_iso: str
    ) -> EventSchemaTypedDict:
        """Build schema from JSON-LD data."""
        # Implementation from original script
        # ... (copy existing implementation)
        pass

    def _populate_derived_fields(self, event_data: EventSchemaTypedDict) -> None:
        """Populates derived fields in the EventSchemaTypedDict."""
        
        now_utc = datetime.utcnow()
        event_data["updatedAt"] = now_utc
        event_data["lastCheckedAt"] = now_utc

        ticket_info = event_data.get("ticketInfo")

        if ticket_info:
            has_price = ticket_info.get("startingPrice") is not None and ticket_info.get("startingPrice", 0) > 0
            display_text = ticket_info.get("displayText")
            has_display_text = bool(display_text.strip()) if display_text else False
            url = ticket_info.get("url")
            has_url = bool(url.strip()) if url else False
            has_tiers = bool(ticket_info.get("tiers"))
            
            event_data["hasTicketInfo"] = has_price or has_display_text or has_url or has_tiers

            is_free_price = ticket_info.get("startingPrice") == 0
            status_text = (ticket_info.get("status") or "").lower()
            display_text_lower = (ticket_info.get("displayText") or "").lower()
            
            is_free_status = "free" in status_text
            is_free_display = "free" in display_text_lower

            if event_data["hasTicketInfo"] and (is_free_price or is_free_status or is_free_display) and not has_price:
                 event_data["isFree"] = True
            else:
                 event_data["isFree"] = False

            sold_out_keywords = ["sold out", "unavailable", "off-sale", "offsale"]
            event_data["isSoldOut"] = any(keyword in status_text for keyword in sold_out_keywords)
            
            # Ensure isFree is False if startingPrice > 0
            starting_price = ticket_info.get("startingPrice")
            if starting_price is not None and starting_price > 0:
                event_data["isFree"] = False

        else:
            event_data["hasTicketInfo"] = False
            event_data["isFree"] = False
            event_data["isSoldOut"] = False

        line_up = event_data.get("lineUp")
        event_data["artistCount"] = len(line_up) if line_up is not None else 0

        images = event_data.get("images")
        event_data["imageCount"] = len(images) if images is not None else 0

    def scrape_event_strategically(self, url: str) -> Dict:
        """Orchestrates scraping with improved methods."""
        # For Ibiza Spotlight, we should use browser first due to JavaScript content
        if self.use_browser and sync_playwright is not None:
            print(f"[INFO] Attempting improved browser fetch for {url}")
            event_data_browser = self.scrape_event_data_improved(url, attempt_with_browser=True)
            if is_data_sufficient(event_data_browser):
                return event_data_browser
        
        # Fallback to requests if browser fails
        print(f"[INFO] Attempting improved requests fetch for {url}")
        event_data_requests = self.scrape_event_data_improved(url, attempt_with_browser=False)
        return event_data_requests

# --- Crawling Logic (adapted from original script) ---

def extract_ibiza_spotlight_event_links(html: str, base_url: str) -> List[str]:
    """Extract event links from Ibiza Spotlight calendar pages."""
    soup = BeautifulSoup(html, "html.parser")
    links = []
    
    # Look for event cards and links
    event_selectors = [
        "a[href*='/night/events/']",
        ".event-card a",
        ".event-listing a",
        "h3 a",  # Event titles are often in h3 tags
        "a:contains('presents')"  # Links containing "presents"
    ]
    
    for selector in event_selectors:
        try:
            elements = soup.select(selector)
            for elem in elements:
                href = elem.get('href')
                if href:
                    # Filter out calendar/navigation links
                    if any(x in href for x in ['/2025/', 'daterange=', '#', 'venues/']):
                        # Allow specific daterange if it's the main listing URL
                        if 'daterange=' in href and urlparse(href).path == urlparse(base_url).path:
                             pass # This is likely the main listing page itself or a paginated version.
                        else:
                            continue # Skip other daterange links, year links, anchors, venue links
                    
                    # Look for actual event pages (typically no query params or simple ones)
                    parsed_href = urlparse(href)
                    if '/night/events/' in parsed_href.path and len(parsed_href.path.split('/')) > 4:
                         # Ensure it's not a month archive or similar
                        if not re.search(r'/night/events/\d{4}/\d{2}$', parsed_href.path):
                            full_url = urljoin(base_url, href)
                            links.append(full_url)
        except:
            continue
    
    # Also look for event titles with links
    for elem in soup.find_all(['h3', 'h4', 'a']):
        text = elem.get_text(strip=True).lower()
        if 'presents' in text or 'opening' in text or 'closing' in text:
            if elem.name == 'a':
                href = elem.get('href')
            else:
                # Check if there's a link inside
                link = elem.find('a')
                href = link.get('href') if link else None
            
            if href and '/night/events/' in href:
                if not re.search(r'/night/events/\d{4}/\d{2}$', urlparse(href).path):
                    full_url = urljoin(base_url, href)
                    links.append(full_url)
    
    # Remove duplicates while preserving order
    seen = set()
    unique_links = []
    for link in links:
        if link not in seen:
            seen.add(link)
            unique_links.append(link)
    
    return unique_links


def crawl_ibiza_spotlight_events(
    listing_url: str,
    scraper: ImprovedMultiLayerEventScraper,
    max_events: int = 0,
    headless: bool = True,
) -> List[EventSchemaTypedDict]:
    """Crawl Ibiza Spotlight listing page (using requests) and scrape events (using improved scraper)."""
    
    print(f"[INFO] Starting crawl of {listing_url} using requests for listing page.")
    
    events = []
    html_content = None

    listing_session = scraper.session
    
    try:
        print(f"[INFO] Fetching listing page with requests: {listing_url}")
        response = listing_session.get(listing_url, timeout=(10, 20))
        response.raise_for_status()
        html_content = response.text
        print(f"[INFO] Successfully fetched listing page, content length: {len(html_content)}")
        
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Failed to fetch listing page {listing_url} with requests: {e}")
        return []
    except Exception as e:
        print(f"[ERROR] Unexpected error fetching listing page {listing_url} with requests: {e}")
        return []

    if not html_content:
        print(f"[ERROR] No HTML content fetched from {listing_url} with requests.")
        return []

    base_url = "https://www.ibiza-spotlight.com"
    event_links = extract_ibiza_spotlight_event_links(html_content, base_url)
    
    print(f"[INFO] Found {len(event_links)} event links from requests-fetched HTML.")
    
    if max_events > 0:
        event_links = event_links[:max_events]
        print(f"[INFO] Limiting to {len(event_links)} events based on max_events={max_events}.")
    
    for idx, event_url in enumerate(event_links, 1):
        print(f"\n[INFO] Scraping event {idx}/{len(event_links)}: {event_url}")
        
        try:
            event_data = scraper.scrape_event_strategically(event_url)
            
            if event_data:
                events.append(event_data)
                title_to_print = event_data.get('title') if isinstance(event_data, dict) and event_data.get('title') else "Unknown Event"
                print(f"[SUCCESS] Scraped: {title_to_print}")
            else:
                print(f"[WARNING] No data extracted for {event_url}")
            
            if idx < len(event_links):
                delay = random.uniform(scraper.random_delay_range[0], scraper.random_delay_range[1])
                print(f"[INFO] Waiting {delay:.1f}s before next event scrape...")
                time.sleep(delay)
                
        except Exception as e:
            print(f"[ERROR] Failed to scrape event detail page {event_url}: {e}")
            continue
            
    return events

# --- End of Crawling Logic ---

def main():
    """Main function with improved scraper and crawl action."""
    parser = argparse.ArgumentParser(
        description="Improved Ibiza Spotlight Event Scraper with better data validation"
    )
    parser.add_argument(
        "action",
        choices=["scrape", "crawl"], # Added "crawl"
        help="Action to perform"
    )
    parser.add_argument(
        "--url",
        default=DEFAULT_TARGET_URL,
        help="URL to scrape or initial URL for crawling (default: %(default)s)"
    )
    parser.add_argument(
        "--max-events", # Added for crawl
        type=int,
        default=0, # Default to 0 (crawl all found on page), original script had 5
        help="Maximum number of events to scrape during crawl (0 for all found on current view)"
    )
    parser.add_argument(
        "--output",
        choices=["json", "markdown", "both"],
        default="both",
        help="Output format (default: %(default)s)"
    )
    parser.add_argument(
        "--output-dir",
        default="output/ibiza_spotlight_improved",
        help="Output directory (default: %(default)s)"
    )
    parser.add_argument(
        "--no-headless",
        action="store_true",
        help="Run browser in non-headless mode"
    )

    args = parser.parse_args()

    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Initialize improved scraper
    scraper = ImprovedMultiLayerEventScraper(
        use_browser=True,
        headless=not args.no_headless
    )

    if args.action == "scrape":
        print(f"Scraping single event with improved methods: {args.url}")
        
        event_data = scraper.scrape_event_strategically(args.url)
        
        if event_data:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if args.output in ["json", "both"]:
                json_file = output_dir / f"improved_event_{timestamp}.json"
                with open(json_file, "w", encoding="utf-8") as f:
                    json.dump(event_data, f, indent=2, default=str, ensure_ascii=False)
                print(f"Saved improved JSON to: {json_file}")
            
            if args.output in ["markdown", "both"]:
                md_file = output_dir / f"improved_event_{timestamp}.md"
                with open(md_file, "w", encoding="utf-8") as f:
                    f.write(format_event_to_markdown(event_data))
                print(f"Saved improved Markdown to: {md_file}")
        else:
            print("No event data extracted")
            
    elif args.action == "crawl":
        print(f"Crawling events from {args.url} with improved methods.")
        all_event_data = crawl_ibiza_spotlight_events(
            listing_url=args.url,
            scraper=scraper,
            max_events=args.max_events,
            headless=not args.no_headless
        )
        
        if all_event_data:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if args.output in ["json", "both"]:
                json_file = output_dir / f"crawled_events_{timestamp}.json"
                with open(json_file, "w", encoding="utf-8") as f:
                    json.dump(all_event_data, f, indent=2, default=str, ensure_ascii=False)
                print(f"Saved {len(all_event_data)} crawled events to JSON: {json_file}")

            if args.output in ["markdown", "both"]:
                md_content = [f"# Crawled Events - {timestamp}\n\n"]
                for i, event_data in enumerate(all_event_data):
                    md_content.append(f"## Event {i+1}\n")
                    md_content.append(format_event_to_markdown(event_data))
                    md_content.append("\n---\n")
                
                md_file = output_dir / f"crawled_events_{timestamp}.md"
                with open(md_file, "w", encoding="utf-8") as f:
                    f.write("\n".join(md_content))
                print(f"Saved {len(all_event_data)} crawled events to Markdown: {md_file}")
        else:
            print("No events extracted during crawl.")


def format_event_to_markdown(event_data: EventSchemaTypedDict) -> str:
    """Format event data to markdown with improved formatting."""
    if not event_data:
        return "# No Event Data Available"
    
    lines = []
    lines.append("# Event Details (Improved Extraction)")
    lines.append("")
    
    title = event_data.get("title")
    if title:
        lines.append(f"**Title:** {title}")
    
    location = event_data.get("location")
    if location and isinstance(location, dict):
        venue = location.get("venue")
        if venue:
            lines.append(f"**Venue:** {venue}")
    
    date_time = event_data.get("dateTime")
    if date_time and isinstance(date_time, dict):
        display_text = date_time.get("displayText")
        if display_text:
            lines.append(f"**Date/Time:** {display_text}")
    
    lineup = event_data.get("lineUp")
    if lineup and isinstance(lineup, list) and lineup:
        lines.append("**Artists:**")
        for artist in lineup:
            if isinstance(artist, dict) and artist.get("name"):
                marker = "🎤" if artist.get("headliner") else "•"
                lines.append(f"  {marker} {artist['name']}")
    
    ticket_info = event_data.get("ticketInfo")
    if ticket_info and isinstance(ticket_info, dict):
        price = ticket_info.get("startingPrice")
        if price:
            lines.append(f"**Price:** €{price}")
    
    lines.append("")
    lines.append("## Extraction Details")
    lines.append(f"- **Method:** {event_data.get('extractionMethod', 'unknown')}")
    lines.append(f"- **Artists Found:** {event_data.get('artistCount', 0)}")
    lines.append(f"- **Has Ticket Info:** {event_data.get('hasTicketInfo', False)}")
    lines.append(f"- **Scraped At:** {event_data.get('scrapedAt', 'unknown')}")
    
    return "\n".join(lines)


if __name__ == "__main__":
    main()