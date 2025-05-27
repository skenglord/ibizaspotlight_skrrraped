import argparse
from datetime import datetime
import json
import random
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, TypedDict

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

DEFAULT_TARGET_URL = "https://ticketsibiza.com/ibiza-calendar/2025-events/"

MODERN_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0",
]

# --- End of embedded convert_to_md ---

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
    doors: Optional[str] # Or consider datetime.time if appropriate

class DateTimeInfoTypedDict(TypedDict, total=False):
    displayText: Optional[str]
    parsed: Optional[ParsedDateTimeTypedDict]
    dayOfWeek: Optional[str]

class ArtistTypedDict(TypedDict, total=False):
    name: str # Mandatory as per schema, though not explicitly stated in issue text
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
    url: str # Primary Key
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
    socialLinks: Optional[Dict[str, str]] # Event specific social links
    fullDescription: Optional[str]
    hasTicketInfo: Optional[bool]
    isFree: Optional[bool]
    isSoldOut: Optional[bool]
    artistCount: Optional[int]
    imageCount: Optional[int]
    scrapedAt: datetime # Mandatory
    updatedAt: Optional[datetime]
    lastCheckedAt: Optional[datetime]
    extractionMethod: Optional[str]
    html: Optional[str] # May be truncated
    extractedData: Optional[Dict] # For fallback debugging
    ticketsUrl: Optional[str] # Direct ticket purchase URL

# End of Type Definitions

def is_data_sufficient(event_data: Dict) -> bool:
    """Checks if the extracted event data is sufficient."""
    if not event_data:
        return False
    # Check if JSON-LD data was found and has a title
    if event_data.get("extractionMethod") == "jsonld" and event_data.get("title"):
        return True
    # Check if fallback data has a title and at least one other key piece of info
    if event_data.get("extractionMethod") == "fallback":
        if event_data.get("title") and (
            event_data.get("location", {}).get("venue")
            or event_data.get("dateTime", {}).get("displayText")
            or event_data.get("ticketInfo", {}).get("startingPrice") > 0
            or event_data.get("fullDescription")
        ): # Added more checks for fallback sufficiency
            return True
    return False


class MultiLayerEventScraper:
    def __init__(
        self,
        use_browser: bool = True,
        headless: bool = True,
        playwright_slow_mo: int = 62,
        random_delay_range: tuple = (0.5, 1.3),
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
        self.rotate_user_agent()  # Initial User-Agent selection and session setup
        # self.session is initialized by rotate_user_agent calling _setup_session

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
            # "TE": "trailers", # Optional, can sometimes cause issues
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
                    page = browser.new_page()
                    # Apply User-Agent to browser context if possible, or ensure session UA is used if not directly fetchable by browser
                    # For now, Playwright uses its own UA management typically. Session UA is for requests.
                    page.goto(url, timeout=30000)
                    content = page.content()
                    browser.close()
                    return content
            except Exception as e:
                print(f"Browser fetch failed for {url}: {e}", file=sys.stderr)
                # Optionally, could fall back to requests here if browser fails mid-operation, but current strategy is attempt-based.
                return None # Explicitly return None on browser failure.
        else:
            # Fallback to requests, or if self.use_browser is False, or if sync_playwright is None, or if use_browser_for_this_fetch is False
            time.sleep(random.uniform(self.random_delay_range[0], self.random_delay_range[1]))
            try:
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
                return response.text
            except Exception as e:
                print(f"Error fetching {url} with requests: {e}", file=sys.stderr)
                return None

    def extract_jsonld_data(self, soup: BeautifulSoup) -> Optional[Dict]:
        """Extract JSON-LD structured data."""
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                raw_ld = script.string or script.get_text()
                data_ld = json.loads(raw_ld)

                graph = data_ld.get("@graph", []) if isinstance(data_ld, dict) else []
                for node in graph:
                    if node.get("@type") == "MusicEvent":
                        return node

                if isinstance(data_ld, dict) and data_ld.get("@type") == "MusicEvent":
                    return data_ld
            except Exception:
                continue
        return None

    def extract_wordpress_data(self, soup: BeautifulSoup) -> Dict:
        """Extract data using WordPress/WooCommerce selectors."""
        data: Dict[str, str] = {}

        title_selectors = [
            "h1.entry-title",
            ".product_title",
            "h1.product-title",
            ".event-title",
            "h1",
        ]
        for selector in title_selectors:
            title_elem = soup.select_one(selector)
            if title_elem:
                data["title"] = title_elem.get_text(strip=True)
                break

        date_selectors = [
            ".event-date",
            ".wcs-event-date",
            ".event-time",
            '[class*="date"]',
            '[class*="time"]',
        ]
        for selector in date_selectors:
            date_elem = soup.select_one(selector)
            if date_elem:
                data["date_text"] = date_elem.get_text(strip=True)
                break

        venue_selectors = [
            ".event-venue",
            ".venue",
            ".location",
            '[class*="venue"]',
            '[class*="location"]',
        ]
        for selector in venue_selectors:
            venue_elem = soup.select_one(selector)
            if venue_elem:
                data["venue"] = venue_elem.get_text(strip=True)
                break

        price_selectors = [
            ".price",
            ".woocommerce-price-amount",
            ".amount",
            '[class*="price"]',
        ]
        for selector in price_selectors:
            price_elem = soup.select_one(selector)
            if price_elem:
                data["price_text"] = price_elem.get_text(strip=True)
                break

        desc_selectors = [
            ".entry-content",
            ".product-description",
            ".event-description",
            ".description",
        ]
        for selector in desc_selectors:
            desc_elem = soup.select_one(selector)
            if desc_elem:
                data["description"] = desc_elem.get_text(strip=True)[:500]
                break

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

    def extract_text_patterns(self, html: str) -> Dict:
        """Extract data using regex patterns."""
        data: Dict[str, str] = {}

        date_patterns = [
            r"(\d{1,2}[/-]\d{1,2}[/-]\d{4})",
            r"(\d{4}-\d{2}-\d{2})",
            r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)[,\s]+(\d{1,2}[/-]\d{1,2}[/-]\d{4})",
        ]
        for pattern in date_patterns:
            match = re.search(pattern, html)
            if match:
                data["date_pattern"] = match.group(0)
                break

        price_patterns = [
            r"[€$£](\d+(?:\.\d{2})?)",
            r"(\d+(?:\.\d{2})?)\s*[€$£]",
            r"Price[:\s]+[€$£]?(\d+(?:\.\d{2})?)",
        ]
        for pattern in price_patterns:
            match = re.search(pattern, html, re.IGNORECASE)
            if match:
                data["price_pattern"] = match.group(0)
                break

        return data
    
    def extract_lineup_from_html(self, soup: BeautifulSoup) -> List[str]:
        """Extract lineup/artists from HTML content."""
        artists = []
        
        # Look for Line Up section
        lineup_headers = soup.find_all(['h3', 'h4', 'h5'], string=re.compile(r'Line\s*Up', re.IGNORECASE))
        for header in lineup_headers:
            # Find the next sibling that contains the lineup
            next_elem = header.find_next_sibling()
            while next_elem and next_elem.name not in ['h3', 'h4', 'h5', 'div']:
                if next_elem.name == 'p':
                    # Extract artists from <br> separated list
                    text = next_elem.get_text(separator='\n', strip=True)
                    potential_artists = [line.strip() for line in text.split('\n') if line.strip()]
                    artists.extend(potential_artists)
                    break
                elif next_elem.name == 'ul':
                    # Extract from list items
                    for li in next_elem.find_all('li'):
                        artist = li.get_text(strip=True)
                        if artist:
                            artists.append(artist)
                    break
                next_elem = next_elem.find_next_sibling()
        
        # Remove duplicates while preserving order
        seen = set()
        unique_artists = []
        for artist in artists:
            if artist not in seen and artist:
                seen.add(artist)
                unique_artists.append(artist)
        
        return unique_artists
    
    def extract_ticket_url_from_html(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract direct ticket purchase URL from HTML."""
        # Look for Buy Tickets button/link
        ticket_links = soup.find_all('a', string=re.compile(r'Buy\s*Tickets', re.IGNORECASE))
        for link in ticket_links:
            href = link.get('href')
            if href and ('fourvenues' in href or 'ticket' in href.lower()):
                return href
        
        # Alternative: look for specific class
        ticket_link = soup.find('a', class_='wcs-btn--action')
        if ticket_link:
            return ticket_link.get('href')
        
        return None

    def scrape_event_data(self, url: str, attempt_with_browser: bool = False) -> Dict:
        """Main scraping method with multiple fallback strategies."""
        html = self.fetch_page(url, use_browser_for_this_fetch=attempt_with_browser)
        if not html:
            return {}

        soup = BeautifulSoup(html, "html.parser")
        now_iso = datetime.utcnow().isoformat() + "Z"

        jsonld_data = self.extract_jsonld_data(soup)
        if jsonld_data:
            # Ensure 'html' key is populated even for jsonld for consistency if needed downstream,
            # though original _build_schema_from_jsonld includes full html.
            # If it's too large, it should be truncated in _build_schema_from_jsonld.
            return self._map_jsonld_to_event_schema(jsonld_data, url, html, now_iso)

        wp_data = self.extract_wordpress_data(soup)
        meta_data = self.extract_meta_data(soup)
        pattern_data = self.extract_text_patterns(html)
        combined_data = {**wp_data, **meta_data, **pattern_data}
        # Ensure 'html' key is populated for fallback, possibly truncated.
        # _build_schema_from_fallback already handles html (truncated).
        return self._map_fallback_to_event_schema(combined_data, url, html, now_iso)

    def scrape_event_strategically(self, url: str) -> Dict:
        """Orchestrates scraping, trying requests first, then Playwright if needed."""
        event_data_requests = self.scrape_event_data(url, attempt_with_browser=False)

        if is_data_sufficient(event_data_requests):
            print(f"[INFO] Data sufficient from requests-only attempt for {url}")
            return event_data_requests

        if self.use_browser and sync_playwright is not None:
            print(
                f"[INFO] Requests-only attempt insufficient for {url}. Attempting with browser."
            )
            event_data_browser = self.scrape_event_data(url, attempt_with_browser=True)
            # Optionally, decide if browser data is "better" even if requests was "sufficient"
            # For now, if requests was sufficient, we don't try browser.
            # If browser data is empty or not better, could return requests data.
            # For simplicity, returning browser data if we attempted it.
            return event_data_browser
        else:
            # Playwright not available or not enabled, return initial requests attempt
            return event_data_requests

    def _map_jsonld_to_event_schema(
        self, node: Dict, url: str, html: str, now_iso: str
    ) -> EventSchemaTypedDict:
        """Build schema from JSON-LD data, populating EventSchemaTypedDict."""
        
        scraped_at_datetime: Optional[datetime] = None
        try:
            scraped_at_datetime = datetime.fromisoformat(now_iso.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            scraped_at_datetime = datetime.utcnow() # Fallback if parsing fails

        # Initialize EventSchemaTypedDict with defaults
        event_data: EventSchemaTypedDict = {
            "url": url,
            "scrapedAt": scraped_at_datetime,
            "extractionMethod": "jsonld",
            "html": html[:5000] if html else None, # Truncate HTML
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
            "hasTicketInfo": False, # Default, to be derived later
            "isFree": False,        # Default, to be derived later
            "isSoldOut": False,     # Default, to be derived later
            "artistCount": None,    # Default, to be derived later
            "imageCount": None,     # Default, to be derived later
            "updatedAt": None,
            "lastCheckedAt": None,
            "extractedData": None, # Not typically used for JSON-LD success
            "ticketsUrl": None
        }

        # --- Populate fields from node ---
        event_data["title"] = node.get("name")

        # Location
        loc_node = node.get("location", {}) or {}
        addr_node = loc_node.get("address", {}) or {}
        geo_node = loc_node.get("geo", {}) or {}
        
        address_parts = [
            addr_node.get("streetAddress"),
            addr_node.get("addressLocality"),
            addr_node.get("addressRegion"),
            addr_node.get("postalCode"),
            addr_node.get("addressCountry"),
        ]
        full_address = " ".join(filter(None, address_parts))
        
        coordinates: Optional[CoordinatesTypedDict] = None
        lat = geo_node.get("latitude")
        lng = geo_node.get("longitude")
        if lat is not None and lng is not None:
            try:
                coordinates = {"lat": float(lat), "lng": float(lng)}
            except (ValueError, TypeError):
                coordinates = None # Or some default if parsing fails
        
        event_data["location"] = {
            "venue": loc_node.get("name"),
            "address": full_address if full_address else None,
            "coordinates": coordinates,
        }

        # DateTime
        start_date_str = node.get("startDate")
        end_date_str = node.get("endDate")
        door_time_str = node.get("doorTime") # Often just time, not full datetime

        parsed_start_date: Optional[datetime] = None
        parsed_end_date: Optional[datetime] = None
        day_of_week: Optional[str] = None

        if start_date_str:
            try:
                parsed_start_date = datetime.fromisoformat(start_date_str.replace("Z", "+00:00"))
                day_of_week = parsed_start_date.strftime("%A")
            except (ValueError, TypeError):
                parsed_start_date = None
        
        if end_date_str:
            try:
                parsed_end_date = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                parsed_end_date = None
        
        # doorTime might be just a time or a full datetime string.
        # For simplicity, keeping as string if not easily parsed as full datetime.
        # More sophisticated parsing could be added if format is known.
        
        date_time_display_parts = []
        if start_date_str: date_time_display_parts.append(start_date_str)
        if end_date_str: date_time_display_parts.append(f"to {end_date_str}")

        event_data["dateTime"] = {
            "displayText": " ".join(date_time_display_parts) if date_time_display_parts else None,
            "parsed": {
                "startDate": parsed_start_date,
                "endDate": parsed_end_date,
                "doors": door_time_str,
            },
            "dayOfWeek": day_of_week,
        }

        # LineUp
        performers_node = node.get("performer", [])
        if isinstance(performers_node, dict): # Handle if performer is single dict
            performers_node = [performers_node]
        
        lineup_list: List[ArtistTypedDict] = []
        for idx, perf_node in enumerate(performers_node):
            if not isinstance(perf_node, dict) or not perf_node.get("name"):
                continue # Skip if performer is not a dict or has no name

            affiliates = perf_node.get("sameAs", [])
            if not isinstance(affiliates, list):
                affiliates = [str(affiliates)] if affiliates else []
            else:
                affiliates = [str(aff) for aff in affiliates]

            genres_perf = perf_node.get("genre", [])
            if isinstance(genres_perf, str):
                genres_perf = [genres_perf]
            elif not isinstance(genres_perf, list):
                genres_perf = []
            else:
                genres_perf = [str(g) for g in genres_perf]

            artist: ArtistTypedDict = {
                "name": str(perf_node["name"]), # Name is mandatory for ArtistTypedDict
                "affiliates": affiliates,
                "genres": genres_perf,
                "headliner": idx == 0, # Simple headliner logic
            }
            lineup_list.append(artist)
        event_data["lineUp"] = lineup_list

        # EventType
        event_type_node = node.get("@type", [])
        if isinstance(event_type_node, str):
            event_data["eventType"] = [event_type_node]
        elif isinstance(event_type_node, list):
            event_data["eventType"] = [str(et) for et in event_type_node]
        else:
            event_data["eventType"] = []


        # Genres (overall event)
        genres_node = node.get("genre", [])
        if isinstance(genres_node, str):
            event_data["genres"] = [genres_node]
        elif isinstance(genres_node, list):
            event_data["genres"] = [str(g) for g in genres_node]
        else:
            event_data["genres"] = []
            

        # TicketInfo
        offers_node = node.get("offers", []) # Can be a single dict or list
        if isinstance(offers_node, dict):
            offers_node = [offers_node]
        
        if offers_node and isinstance(offers_node, list):
            first_offer = offers_node[0] if offers_node else {}
            
            starting_price: Optional[float] = None
            prices = []
            for offer_item in offers_node:
                if isinstance(offer_item, dict) and offer_item.get("price") is not None:
                    try:
                        prices.append(float(offer_item.get("price", 0)))
                    except (ValueError, TypeError):
                        pass
            if prices:
                starting_price = min(prices)

            ticket_tiers: List[TicketTierTypedDict] = []
            for tier_offer in offers_node:
                if not isinstance(tier_offer, dict): continue
                tier_price: Optional[float] = None
                try:
                    tier_price = float(tier_offer.get("price")) if tier_offer.get("price") is not None else None
                except (ValueError, TypeError):
                    pass # Keep as None
                
                availability = tier_offer.get("availability", "")
                is_available = "instock" in availability.lower() if availability else None

                tier: TicketTierTypedDict = {
                    "name": tier_offer.get("name") or tier_offer.get("category"),
                    "price": tier_price,
                    "available": is_available,
                }
                ticket_tiers.append(tier)

            event_data["ticketInfo"] = {
                "displayText": first_offer.get("name") or first_offer.get("description"),
                "startingPrice": starting_price,
                "currency": first_offer.get("priceCurrency"),
                "tiers": ticket_tiers,
                "status": first_offer.get("availability"),
                "url": first_offer.get("url"),
            }

        # Organizer
        organizer_node = node.get("organizer", {})
        if isinstance(organizer_node, list): # Take first if list
            organizer_node = organizer_node[0] if organizer_node else {}
        
        if isinstance(organizer_node, dict):
            org_affiliates = organizer_node.get("sameAs", [])
            if not isinstance(org_affiliates, list):
                org_affiliates = [str(org_affiliates)] if org_affiliates else []
            else:
                org_affiliates = [str(aff) for aff in org_affiliates]
            
            # Basic social link extraction from sameAs if they are URLs
            org_socials = {}
            for aff_url in org_affiliates:
                if "facebook.com" in aff_url: org_socials["facebook"] = aff_url
                elif "twitter.com" in aff_url or "x.com" in aff_url: org_socials["twitter"] = aff_url
                elif "instagram.com" in aff_url: org_socials["instagram"] = aff_url
            
            event_data["organizer"] = {
                "name": organizer_node.get("name"),
                "affiliates": org_affiliates,
                "socialLinks": org_socials,
            }

        # Age Restriction
        event_data["ageRestriction"] = node.get("typicalAgeRange")

        # Images
        images_node = node.get("image", [])
        if isinstance(images_node, str):
            event_data["images"] = [images_node]
        elif isinstance(images_node, list):
            event_data["images"] = [str(img) for img in images_node if isinstance(img, str)]
        else:
            event_data["images"] = []


        # Event-specific Social Links (if any, distinct from organizer)
        # This is a simple interpretation; JSON-LD doesn't have a standard top-level socialLinks for events
        event_same_as = node.get("sameAs", [])
        if isinstance(event_same_as, str): event_same_as = [event_same_as]
        if isinstance(event_same_as, list):
            ev_socials = {}
            for s_url in event_same_as:
                if not isinstance(s_url, str): continue
                if "facebook.com" in s_url and "facebook" not in event_data["socialLinks"]: ev_socials["facebook"] = s_url
                elif ("twitter.com" in s_url or "x.com" in s_url) and "twitter" not in event_data["socialLinks"]: ev_socials["twitter"] = s_url
                elif "instagram.com" in s_url and "instagram" not in event_data["socialLinks"]: ev_socials["instagram"] = s_url
            if ev_socials: # Only add if different from organizer's
                 event_data["socialLinks"].update(ev_socials)


        # Full Description
        description = node.get("description")
        if description:
            # The description is usually plain text in JSON-LD, not HTML
            event_data["fullDescription"] = str(description)
        
        # Promos - JSON-LD doesn't have a standard field. Placeholder.
        # event_data["promos"] = []
        
        # Extract additional data from HTML that's not in JSON-LD
        if html:
            soup = BeautifulSoup(html, "html.parser")
            
            # Extract full lineup from HTML
            html_artists = self.extract_lineup_from_html(soup)
            if html_artists:
                # Merge with existing lineup, avoiding duplicates
                existing_names = {artist["name"] for artist in event_data["lineUp"]}
                for idx, artist_name in enumerate(html_artists):
                    if artist_name not in existing_names:
                        event_data["lineUp"].append({
                            "name": artist_name,
                            "affiliates": [],
                            "genres": [],
                            "headliner": False  # Additional artists are not headliners
                        })
            
            # Extract ticket URL from HTML
            ticket_url = self.extract_ticket_url_from_html(soup)
            if ticket_url:
                event_data["ticketsUrl"] = ticket_url
        
        # Populate derived fields
        self._populate_derived_fields(event_data)
        
        return event_data

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

            if event_data["hasTicketInfo"] and (is_free_price or is_free_status or is_free_display) and not has_price : # Price > 0 overrides free status
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

    def _map_fallback_to_event_schema(
        self, data: Dict, url: str, html: str, now_iso: str
    ) -> EventSchemaTypedDict:
        """Build schema from fallback extraction methods, populating EventSchemaTypedDict."""

        scraped_at_datetime: Optional[datetime] = None
        try:
            scraped_at_datetime = datetime.fromisoformat(now_iso.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            scraped_at_datetime = datetime.utcnow() # Fallback if parsing fails

        # Initialize EventSchemaTypedDict with defaults
        event_data: EventSchemaTypedDict = {
            "url": url,
            "scrapedAt": scraped_at_datetime,
            "extractionMethod": "fallback",
            "html": html[:5000] if html else None, # Truncate HTML
            "extractedData": data, # Store the raw fallback data
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
            "ticketsUrl": None,
        }

        # --- Populate fields from data (fallback extraction results) ---
        event_data["title"] = data.get("title")

        # Location
        event_data["location"] = {
            "venue": data.get("venue"),
            "address": data.get("address"), # Assuming 'address' key might exist from some fallback
            "coordinates": None, # Fallback rarely provides this directly
        }

        # DateTime
        date_text = data.get("date_text") or data.get("date_pattern")
        # Basic date parsing from text is complex; for now, we store display text.
        # A more sophisticated date parsing utility would be needed for robust startDate/endDate.
        # For simplicity, ParsedDateTimeTypedDict might be largely None from fallback.
        event_data["dateTime"] = {
            "displayText": date_text,
            "parsed": { # Typically None or very basic from fallback
                "startDate": None,
                "endDate": None,
                "doors": None,
            },
            "dayOfWeek": None, # Would require successful startDate parsing
        }

        # LineUp - Fallback usually doesn't provide structured lineup
        # If data.get("artists") existed and was a list of strings:
        # event_data["lineUp"] = [{"name": artist_name} for artist_name in data.get("artists", [])]
        event_data["lineUp"] = []


        # EventType & Genres - Typically not available from basic fallback
        event_data["eventType"] = []
        event_data["genres"] = []

        # TicketInfo
        price_text = data.get("price_text") or data.get("price_pattern")
        starting_price: Optional[float] = None
        currency: Optional[str] = None

        if price_text:
            # Attempt to extract first number as price
            price_match = re.search(r"(\d+(?:\.\d{2})?)", price_text)
            if price_match:
                try:
                    starting_price = float(price_match.group(1))
                except ValueError:
                    starting_price = None
            
            # Attempt to infer currency
            if "€" in price_text: currency = "EUR"
            elif "$" in price_text: currency = "USD"
            elif "£" in price_text: currency = "GBP"
        
        event_data["ticketInfo"] = {
            "displayText": price_text,
            "startingPrice": starting_price,
            "currency": currency,
            "tiers": [], # Fallback rarely provides structured tiers
            "status": None, # Could try to infer "Sold Out" etc. if specific text patterns exist
            "url": data.get("ticket_url"),
        }
        
        # Organizer - Typically not available from basic fallback
        event_data["organizer"] = None
        
        # AgeRestriction - Typically not available from basic fallback
        event_data["ageRestriction"] = None

        # Images - Primarily from OpenGraph if available in fallback data
        og_image = data.get("image") # Assuming 'image' key from extract_meta_data (OpenGraph)
        if og_image and isinstance(og_image, str):
            event_data["images"] = [og_image]
        else:
            event_data["images"] = []
            
        # SocialLinks - Typically not available from basic fallback
        event_data["socialLinks"] = {}

        # Full Description
        description_html = data.get("description") or data.get("meta_description")
        if description_html:
            event_data["fullDescription"] = str(description_html)  # Usually already plain text from meta tags
        else:
            event_data["fullDescription"] = None
        
        # Extract additional data from HTML
        if html:
            soup = BeautifulSoup(html, "html.parser")
            
            # Extract lineup from HTML
            html_artists = self.extract_lineup_from_html(soup)
            if html_artists:
                event_data["lineUp"] = [{"name": artist_name, "affiliates": [], "genres": [], "headliner": idx == 0}
                                       for idx, artist_name in enumerate(html_artists)]
            
            # Extract ticket URL from HTML
            ticket_url = self.extract_ticket_url_from_html(soup)
            if ticket_url:
                event_data["ticketsUrl"] = ticket_url
        
        # Populate derived fields
        self._populate_derived_fields(event_data)
            
        return event_data


def crawl_listing_for_events(
    listing_url: str,
    scraper: "MultiLayerEventScraper",
    max_pages: int = 4000,
    *,
    headless: bool = True,
) -> List[Dict]:
    """Crawl a listing page and scrape each linked event."""
    if sync_playwright is None:
        print("Playwright is not installed; cannot crawl listing", file=sys.stderr)
        return []

    scraped: List[Dict] = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()
        page.goto(listing_url, timeout=30000)
        try:
            page.wait_for_selector("text=INFO", timeout=10000)
        except Exception:
            pass
        links = [
            elem.get_attribute("href")
            for elem in page.query_selector_all("text=INFO")
            if elem.get_attribute("href")
        ]
        browser.close()

    count = 0
    for link in links:
        if count >= max_pages:
            break
        print(f"Scraping: {link}")

        scraper.pages_scraped_since_ua_rotation += 1
        if scraper.pages_scraped_since_ua_rotation >= scraper.rotate_ua_after_pages:
            scraper.rotate_user_agent()
            print(
                f"[INFO] Rotating User-Agent during crawl to: {scraper.current_user_agent}"
            )
        # Random delay is now in fetch_page for requests-based fetching

        data = scraper.scrape_event_strategically(link)
        if data:
            scraped.append(data)
            print(f"✓ Extracted data using: {data.get('extractionMethod', 'unknown')}")
        else:
            print("✗ No data extracted")
        count += 1

    return scraped


# Helper for JSON serialization of datetime objects
def datetime_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

def format_event_to_markdown(event_data: EventSchemaTypedDict) -> str:
    """Formats a single EventSchemaTypedDict object into a Markdown string."""
    md_parts = []

    md_parts.append(f"## {event_data.get('title', 'Unknown Event')}\n")
    md_parts.append(f"**URL:** {event_data.get('url', 'N/A')}\n")

    if event_data.get('extractionMethod'):
        md_parts.append(f"**Extraction Method:** {event_data.get('extractionMethod')}\n")
    
    md_parts.append(f"**Scraped At:** {event_data.get('scrapedAt').isoformat() if event_data.get('scrapedAt') else 'N/A'}\n")
    if event_data.get('updatedAt'):
        md_parts.append(f"**Updated At:** {event_data.get('updatedAt').isoformat()}\n")


    # Location
    loc = event_data.get("location")
    if loc:
        md_parts.append("\n### Location\n")
        if loc.get("venue"): md_parts.append(f"- **Venue:** {loc.get('venue')}\n")
        if loc.get("address"): md_parts.append(f"- **Address:** {loc.get('address')}\n")
        coords = loc.get("coordinates")
        if coords and coords.get("lat") is not None and coords.get("lng") is not None:
            md_parts.append(f"- **Coordinates:** Lat: {coords.get('lat')}, Lng: {coords.get('lng')}\n")

    # DateTime
    dt_info = event_data.get("dateTime")
    if dt_info:
        md_parts.append("\n### Date & Time\n")
        if dt_info.get("displayText"): md_parts.append(f"- **Display Text:** {dt_info.get('displayText')}\n")
        parsed_dt = dt_info.get("parsed")
        if parsed_dt:
            if parsed_dt.get("startDate"): md_parts.append(f"- **Start Date:** {parsed_dt.get('startDate').isoformat()}\n")
            if parsed_dt.get("endDate"): md_parts.append(f"- **End Date:** {parsed_dt.get('endDate').isoformat()}\n")
            if parsed_dt.get("doors"): md_parts.append(f"- **Doors Open:** {parsed_dt.get('doors')}\n")
        if dt_info.get("dayOfWeek"): md_parts.append(f"- **Day of Week:** {dt_info.get('dayOfWeek')}\n")

    # LineUp
    lineup = event_data.get("lineUp")
    if lineup:
        md_parts.append("\n### Lineup\n")
        for artist in lineup:
            artist_name = artist.get('name', 'Unknown Artist')
            md_parts.append(f"- **{artist_name}**")
            details = []
            if artist.get('headliner'): details.append("Headliner")
            if artist.get('genres'): details.append(f"Genres: {', '.join(artist.get('genres'))}")
            if artist.get('affiliates'): details.append(f"Links: {', '.join(artist.get('affiliates'))}")
            if details: md_parts.append(f" ({', '.join(details)})\n")
            else: md_parts.append("\n")
        md_parts.append(f"\n_Total Artists: {event_data.get('artistCount', 0)}_\n")


    # TicketInfo
    ticket_info = event_data.get("ticketInfo")
    if ticket_info:
        md_parts.append("\n### Ticket Information\n")
        if ticket_info.get("displayText"): md_parts.append(f"- **Display Text:** {ticket_info.get('displayText')}\n")
        if ticket_info.get("startingPrice") is not None:
            price_str = f"{ticket_info.get('startingPrice', 0)}"
            if ticket_info.get("currency"): price_str += f" {ticket_info.get('currency')}"
            md_parts.append(f"- **Starting Price:** {price_str}\n")
        if ticket_info.get("status"): md_parts.append(f"- **Status:** {ticket_info.get('status')}\n")
        if ticket_info.get("url"): md_parts.append(f"- **Ticket URL:** [Link]({ticket_info.get('url')})\n")
        
        tiers = ticket_info.get("tiers")
        if tiers:
            md_parts.append("- **Tiers:**\n")
            for tier in tiers:
                tier_name = tier.get('name', 'N/A Tier')
                tier_price = tier.get('price', 'N/A')
                tier_avail = "Available" if tier.get('available', False) else ("Not Available" if tier.get('available') is False else "N/A")
                md_parts.append(f"  - {tier_name}: {tier_price} ({tier_avail})\n")
        
        md_parts.append(f"- Has Ticket Info: {'Yes' if event_data.get('hasTicketInfo') else 'No'}\n")
        md_parts.append(f"- Is Free: {'Yes' if event_data.get('isFree') else 'No'}\n")
        md_parts.append(f"- Is Sold Out: {'Yes' if event_data.get('isSoldOut') else 'No'}\n")
    
    # Direct Ticket Purchase URL
    if event_data.get("ticketsUrl"):
        if not ticket_info:
            md_parts.append("\n### Ticket Information\n")
        md_parts.append(f"- **Buy Tickets:** [Direct Link]({event_data.get('ticketsUrl')})\n")


    # Organizer
    organizer = event_data.get("organizer")
    if organizer:
        md_parts.append("\n### Organizer\n")
        if organizer.get("name"): md_parts.append(f"- **Name:** {organizer.get('name')}\n")
        if organizer.get("affiliates"): md_parts.append(f"- **Affiliates:** {', '.join(organizer.get('affiliates'))}\n")
        org_socials = organizer.get("socialLinks")
        if org_socials:
            md_parts.append("- **Socials:** \n")
            for site, link in org_socials.items():
                md_parts.append(f"  - [{site.capitalize()}]({link})\n")

    # Other details
    if event_data.get("eventType"):
        md_parts.append(f"\n**Event Type(s):** {', '.join(event_data.get('eventType'))}\n")
    if event_data.get("genres"): # Overall event genres
        md_parts.append(f"**Overall Genre(s):** {', '.join(event_data.get('genres'))}\n")
    if event_data.get("ageRestriction"):
        md_parts.append(f"**Age Restriction:** {event_data.get('ageRestriction')}\n")
    if event_data.get("promos"):
        md_parts.append(f"**Promos:** {', '.join(event_data.get('promos'))}\n")

    # Images
    images = event_data.get("images")
    if images:
        md_parts.append("\n### Images\n")
        for img_url in images:
            md_parts.append(f"- [Image Link]({img_url})\n")
        md_parts.append(f"\n_Total Images: {event_data.get('imageCount', 0)}_\n")

    # Event-specific Social Links
    event_socials = event_data.get("socialLinks")
    if event_socials and (not organizer or event_socials != organizer.get("socialLinks")): # Avoid duplicate if same as organizer
        md_parts.append("\n### Event Social Links\n")
        for site, link in event_socials.items():
            md_parts.append(f"- [{site.capitalize()}]({link})\n")

    # Full Description
    if event_data.get("fullDescription"):
        md_parts.append("\n### Full Description\n")
        # Truncate for summary, or show full? For now, full.
        md_parts.append(f"{event_data.get('fullDescription')}\n")
    
    # For debugging fallback
    # if event_data.get("extractedData"):
    #     md_parts.append("\n### Raw Extracted Data (Fallback)\n")
    #     md_parts.append(f"```json\n{json.dumps(event_data.get('extractedData'), indent=2, default=datetime_serializer)}\n```\n")

    return "".join(md_parts)


def fetch_and_parse(
    url: str,
    format: str = "JSON",
    use_llm: bool = False,
    *,
    use_browser: bool = True,
    headless: bool = True,
):
    """Fetch and parse a single event URL."""
    scraper = MultiLayerEventScraper(use_browser=use_browser, headless=headless)
    event_data = scraper.scrape_event_data(url)

    if format and format.lower().startswith("mark"):
        html = event_data.get("html", "")
        return convert_to_md(html)
    return event_data


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description="Scrape Ticketmaster event pages")
    parser.add_argument(
        "events_path", nargs="?", help="Path to events JSON file (fallback)"
    )
    parser.add_argument(
        "--target-url",
        default=DEFAULT_TARGET_URL,
        help="Direct event URL to scrape or listing page when crawling",
    )
    parser.add_argument(
        "--crawl-listing",
        action="store_true",
        help="Crawl listing page for INFO links",
    )
    browser_group = parser.add_mutually_exclusive_group()
    browser_group.add_argument(
        "--headless",
        dest="headless",
        action="store_true",
        help="Launch browser in headless mode (default)",
    )
    browser_group.add_argument(
        "--show-browser",
        dest="headless",
        action="store_false",
        help="Show browser window",
    )
    parser.set_defaults(headless=True)
    parser.add_argument(
        "--no-browser",
        action="store_true",
        help="Disable browser usage and fall back to requests",
    )
    parser.add_argument(
        "--playwright-slow-mo",
        type=int,
        default=62,
        help="Slow down Playwright operations by N milliseconds. Default: 62",
    )
    parser.add_argument(
        "--min-request-delay",
        type=float,
        default=0.5,
        help="Minimum random delay in seconds between requests (for non-Playwright fetches). Default: 0.5",
    )
    parser.add_argument(
        "--max-request-delay",
        type=float,
        default=1.3,
        help="Maximum random delay in seconds between requests (for non-Playwright fetches). Default: 1.3",
    )
    parser.add_argument(
        "--user-agents-file",
        type=str,
        default=None,
        help="Path to a file containing User-Agent strings (one per line). Overrides default list.",
    )
    args = parser.parse_args()

    user_agents_list = MODERN_USER_AGENTS  # Default
    if args.user_agents_file:
        try:
            with open(args.user_agents_file, "r") as f:
                user_agents_list = [line.strip() for line in f if line.strip()]
            if not user_agents_list:
                print(
                    f"[WARNING] User agents file {args.user_agents_file} was empty. Using default user agents.",
                    file=sys.stderr,
                )
                user_agents_list = MODERN_USER_AGENTS
        except FileNotFoundError:
            print(
                f"[WARNING] User agents file {args.user_agents_file} not found. Using default user agents.",
                file=sys.stderr,
            )
            user_agents_list = MODERN_USER_AGENTS

    scraper = MultiLayerEventScraper(
        use_browser=not args.no_browser,
        headless=args.headless,
        playwright_slow_mo=args.playwright_slow_mo,
        random_delay_range=(args.min_request_delay, args.max_request_delay),
        user_agents=user_agents_list,
    )

    default_events_path = (
        Path(__file__).resolve().parent / "ticketsibiza_event_data.json"
    )
    events_path = Path(args.events_path) if args.events_path else default_events_path

    if args.crawl_listing:
        all_event_data = crawl_listing_for_events(
            args.target_url,
            scraper,
            max_pages=4000,
            headless=args.headless,
        )
    else:
        if args.target_url:
            event_urls = [args.target_url]
        else:
            try:
                with events_path.open() as f:
                    events = json.load(f)
                    event_urls = [
                        e.get("url")
                        for e in events
                        if isinstance(e, dict) and e.get("url")
                    ]
            except FileNotFoundError:
                print(f"{events_path} not found.", file=sys.stderr)
                return

        if not event_urls:
            print("No event URLs to process.", file=sys.stderr)
            return

        all_event_data: List[Dict] = []
        max_pages = 4000
        scraped_count = 0

        for url in event_urls:
            if scraped_count >= max_pages:
                break
            print(f"Scraping: {url}")

            scraper.pages_scraped_since_ua_rotation += 1
            if scraper.pages_scraped_since_ua_rotation >= scraper.rotate_ua_after_pages:
                scraper.rotate_user_agent()
                print(f"[INFO] Rotating User-Agent to: {scraper.current_user_agent}")
            # Random delay is now in fetch_page for requests-based fetching

            event_data = scraper.scrape_event_strategically(url)
            if event_data:
                all_event_data.append(event_data)
                print(
                    f"✓ Extracted data using: {event_data.get('extractionMethod', 'unknown')}"
                )
            else:
                print("✗ No data extracted")
            scraped_count += 1

    output = {"events": all_event_data}
    with open("ticketsibiza_scraped_data.json", "w") as f:
        json.dump(output, f, indent=2, default=datetime_serializer)

    markdown_content = "# TicketsIbiza Scraped Data (New Schema)\n\n"
    for ev_data in all_event_data: # ev_data is an EventSchemaTypedDict
        markdown_content += format_event_to_markdown(ev_data)
        markdown_content += "\n---\n\n" # Separator
        
    with open("ticketsibiza_event_data_parsed.md", "w") as f:
        f.write(markdown_content)

    print(f"\n✓ Scraped {len(all_event_data)} events")
    print("✓ Data saved to ticketsibiza_scraped_data.json")
    print("✓ Markdown saved to ticketsibiza_event_data_parsed.md")


if __name__ == "__main__":
    main()
