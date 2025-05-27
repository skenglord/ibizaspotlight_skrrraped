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

    def fetch_page(self, url: str, use_browser_for_this_fetch: bool = False, max_retries: int = 3, initial_delay: float = 2.0) -> Optional[str]:
        """Fetch page HTML with error handling and strategic browser use, including retries."""
        if self.use_browser and use_browser_for_this_fetch and sync_playwright is not None:
            for attempt in range(max_retries):
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
                        
                        print(f"[INFO] Navigating to {url} (Attempt {attempt + 1}/{max_retries})")
                        page.goto(url, timeout=60000) # Increased timeout for robustness
                        
                        # Handle cookie consent
                        try:
                            cookie_button = page.locator('text="NO PROBLEM"').first
                            if cookie_button.is_visible(timeout=5000): # Increased timeout
                                print("[INFO] Accepting cookies...")
                                cookie_button.click()
                                time.sleep(2) # Increased sleep
                        except Exception:
                            print("[INFO] No cookie banner or already accepted.")
                        
                        # Wait for content to load, using a more specific selector if possible
                        # Fallback to 'body' if specific selectors are not always present
                        try:
                            page.wait_for_selector('body', timeout=10000) # Wait for body to be present
                            page.wait_for_load_state('networkidle')
                            # Consider adding a wait for a specific event listing element here
                            # e.g., page.wait_for_selector('.event-card', timeout=5000)
                        except Exception as e:
                            print(f"[WARNING] Could not wait for specific selector or networkidle: {e}", file=sys.stderr)
                            # Continue anyway, content might still be available
                        
                        content = page.content()
                        browser.close()
                        return content
                except Exception as e:
                    print(f"[ERROR] Browser fetch failed for {url} on attempt {attempt + 1}: {e}", file=sys.stderr)
                    if attempt < max_retries - 1:
                        sleep_time = initial_delay * (2 ** attempt)
                        print(f"[INFO] Retrying in {sleep_time:.2f} seconds...", file=sys.stderr)
                        time.sleep(sleep_time)
                    else:
                        print(f"[ERROR] Max retries reached for {url}.", file=sys.stderr)
                        return None
        else:
            # Fallback to requests
            time.sleep(random.uniform(self.random_delay_range[0], self.random_delay_range[1]))
            try:
                response = self.session.get(url, timeout=15) # Increased timeout
                response.raise_for_status()
                return response.text
            except Exception as e:
                print(f"[ERROR] Error fetching {url} with requests: {e}", file=sys.stderr)
                return None

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

        # Date and time extraction
        date_selectors = [
            ".event-date",
            ".date",
            "[data-date]",
            '[class*="date"]'
        ]
        for selector in date_selectors:
            try:
                date_elem = soup.select_one(selector)
                if date_elem:
                    data["date_text"] = date_elem.get_text(strip=True)
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

        # Price extraction
        price_selectors = [
            ".price",
            ".ticket-price",
            '[class*="price"]',
            ':contains("€")'
        ]
        for selector in price_selectors:
            try:
                price_elems = soup.select(selector)
                for elem in price_elems:
                    text = elem.get_text(strip=True)
                    if "€" in text:
                        data["price_text"] = text
                        break
                if "price_text" in data:
                    break
            except:
                continue

        # Artist/lineup extraction
        artist_selectors = [
            ".artist",
            ".performer", 
            ".lineup",
            ".artist-name"
        ]
        for selector in artist_selectors:
            try:
                artist_elem = soup.select_one(selector)
                if artist_elem:
                    data["artist"] = artist_elem.get_text(strip=True)
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

    def extract_text_patterns(self, html: str) -> Dict:
        """Extract data using regex patterns specific to Ibiza Spotlight."""
        data: Dict[str, str] = {}

        # Date patterns for Ibiza Spotlight format
        date_patterns = [
            r"(\w{3}\s\d{1,2}\s\w{3})",  # "Mon 30 Jun"
            r"(\d{2}:\d{2})",             # "23:30"
            r"(\d{1,2}[/-]\d{1,2}[/-]\d{4})",
            r"(\d{4}-\d{2}-\d{2})",
        ]
        for pattern in date_patterns:
            match = re.search(pattern, html)
            if match:
                data["date_pattern"] = match.group(0)
                break

        # Price patterns for Euro currency
        price_patterns = [
            r"(\d+€)",                    # "65€", "85€"
            r"€(\d+(?:\.\d{2})?)",
            r"(\d+(?:\.\d{2})?)\s*€",
            r"(Early Entry.*?€\d+)",      # "Early Entry Ticket before 01:00"
            r"(General Admission.*?€\d+)", # "General Admission 2nd Release"
        ]
        for pattern in price_patterns:
            match = re.search(pattern, html, re.IGNORECASE)
            if match:
                data["price_pattern"] = match.group(0)
                break

        # Venue patterns
        venue_patterns = [
            r"\[([A-Z]+)\]",              # "[UNVRS]", "[VENUE]"
            r"at\s+([A-Za-z\s]+?)(?:\s*-|\s*\|)",
        ]
        for pattern in venue_patterns:
            match = re.search(pattern, html)
            if match:
                data["venue_pattern"] = match.group(1)
                break

        return data
    
    def extract_lineup_from_html(self, soup: BeautifulSoup) -> List[str]:
        """Extract lineup/artists from HTML content."""
        artists = []
        
        # Look for artist mentions in various contexts
        artist_patterns = [
            r"([A-Za-z\s]+?)\s*\+\s*more\s*TBA",  # "Eric Prydz + more TBA"
            r"presents\s+([^-]+)",                  # After "presents"
        ]
        
        text = soup.get_text()
        for pattern in artist_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                artist = match.strip()
                if artist and len(artist) > 2:  # Filter out short matches
                    artists.append(artist)
        
        # Also check specific elements
        for elem in soup.find_all(text=re.compile(r'\+\s*more\s*TBA', re.IGNORECASE)):
            parent_text = elem.parent.get_text(strip=True)
            # Extract artist name before "+ more TBA"
            match = re.search(r'([A-Za-z\s]+?)\s*\+\s*more\s*TBA', parent_text)
            if match:
                artists.append(match.group(1).strip())
        
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
        ticket_selectors = [
            'a:contains("BUY TICKETS")',
            'a:contains("ADD TO BASKET")',
            'a.buy-tickets',
            'a[href*="ticket"]',
            '.ticket-link a'
        ]
        
        for selector in ticket_selectors:
            try:
                links = soup.select(selector)
                for link in links:
                    href = link.get('href')
                    if href:
                        return urljoin("https://www.ibiza-spotlight.com", href)
            except:
                continue
        
        return None

    def scrape_event_data(self, url: str, attempt_with_browser: bool = False) -> Dict:
        """Main scraping method with multiple fallback strategies."""
        html = self.fetch_page(url, use_browser_for_this_fetch=attempt_with_browser)
        if not html:
            return {}

        soup = BeautifulSoup(html, "html.parser")
        now_iso = datetime.utcnow().isoformat() + "Z"

        # Try JSON-LD first
        jsonld_data = self.extract_jsonld_data(soup)
        if jsonld_data:
            return self._map_jsonld_to_event_schema(jsonld_data, url, html, now_iso)

        # Try Ibiza Spotlight specific extraction
        ibiza_data = self.extract_ibiza_spotlight_data(soup)
        meta_data = self.extract_meta_data(soup)
        pattern_data = self.extract_text_patterns(html)
        combined_data = {**ibiza_data, **meta_data, **pattern_data}
        
        return self._map_fallback_to_event_schema(combined_data, url, html, now_iso, soup)

    def scrape_event_strategically(self, url: str) -> Dict:
        """Orchestrates scraping, trying requests first, then Playwright if needed."""
        # For Ibiza Spotlight, we should use browser first due to JavaScript content
        if self.use_browser and sync_playwright is not None:
            print(f"[INFO] Attempting browser fetch for {url}")
            event_data_browser = self.scrape_event_data(url, attempt_with_browser=True)
            if is_data_sufficient(event_data_browser):
                return event_data_browser
        
        # Fallback to requests if browser fails
        print(f"[INFO] Attempting requests fetch for {url}")
        event_data_requests = self.scrape_event_data(url, attempt_with_browser=False)
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
        self, data: Dict, url: str, html: str, now_iso: str, soup: BeautifulSoup = None
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
        venue = data.get("venue") or data.get("venue_pattern")
        event_data["location"] = {
            "venue": venue,
            "address": data.get("address"), 
            "coordinates": None,
        }

        # DateTime
        date_text = data.get("date_text") or data.get("date_pattern")
        # Parse Ibiza Spotlight date format
        parsed_date = None
        day_of_week = None
        if date_text:
            # Try to parse "Mon 30 Jun" format
            match = re.search(r"(\w{3})\s+(\d{1,2})\s+(\w{3})", date_text)
            if match:
                day_name = match.group(1)
                day = match.group(2)
                month = match.group(3)
                # For now, assume current year or 2025
                year = "2025"
                try:
                    # Convert month name to number
                    months = {"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
                             "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12}
                    if month in months:
                        parsed_date = datetime(int(year), months[month], int(day))
                        day_of_week = day_name
                except:
                    pass
        
        # Extract time if available
        time_match = re.search(r"(\d{2}:\d{2})", date_text) if date_text else None
        time_str = time_match.group(1) if time_match else None
        
        event_data["dateTime"] = {
            "displayText": date_text,
            "parsed": {
                "startDate": parsed_date,
                "endDate": None,
                "doors": time_str,
            },
            "dayOfWeek": day_of_week,
        }

        # LineUp - Extract from patterns or soup
        if soup:
            html_artists = self.extract_lineup_from_html(soup)
            event_data["lineUp"] = [{"name": artist, "affiliates": [], "genres": [], "headliner": idx == 0} 
                                   for idx, artist in enumerate(html_artists)]
        elif data.get("artist"):
            # Parse artist field
            artist_text = data.get("artist", "")
            artists = []
            if "+ more TBA" in artist_text:
                main_artist = artist_text.split("+ more TBA")[0].strip()
                if main_artist:
                    artists.append(main_artist)
            else:
                artists.append(artist_text)
            
            event_data["lineUp"] = [{"name": artist, "affiliates": [], "genres": [], "headliner": idx == 0} 
                                   for idx, artist in enumerate(artists)]

        # TicketInfo - Parse price patterns
        price_text = data.get("price_text") or data.get("price_pattern")
        if price_text:
            # Extract prices and tier names
            prices = []
            tiers = []
            
            # Look for Early Entry pattern
            early_match = re.search(r"(\d+)€.*?Early Entry", price_text, re.IGNORECASE)
            if early_match:
                price = float(early_match.group(1))
                prices.append(price)
                tiers.append({
                    "name": "Early Entry Ticket before 01:00",
                    "price": price,
                    "available": True
                })
            
            # Look for General Admission pattern
            ga_match = re.search(r"(\d+)€.*?General Admission", price_text, re.IGNORECASE)
            if ga_match:
                price = float(ga_match.group(1))
                prices.append(price)
                tiers.append({
                    "name": "General Admission 2nd Release",
                    "price": price,
                    "available": True
                })
            
            # Generic price extraction if no specific patterns
            if not tiers:
                price_matches = re.findall(r"(\d+)€", price_text)
                for price_str in price_matches:
                    price = float(price_str)
                    prices.append(price)
                    tiers.append({
                        "name": f"{price}€ Ticket",
                        "price": price,
                        "available": True
                    })
            
            if prices:
                event_data["ticketInfo"] = {
                    "displayText": price_text,
                    "startingPrice": min(prices) if prices else None,
                    "currency": "EUR",
                    "tiers": tiers,
                    "status": None,
                    "url": None,
                }

        # Images from meta
        if data.get("image"):
            event_data["images"] = [data["image"]]

        # Description
        event_data["fullDescription"] = data.get("description") or data.get("meta_description")

        # Extract ticket URL if soup available
        if soup:
            ticket_url = self.extract_ticket_url_from_html(soup)
            if ticket_url:
                event_data["ticketsUrl"] = ticket_url

        # Populate derived fields
        self._populate_derived_fields(event_data)

        return event_data


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
                        continue
                    # Look for actual event pages
                    if '/night/events/' in href and len(href.split('/')) > 4:
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
    max_events: int = 5,
    headless: bool = True,
    output_format: str = "json"
) -> List[EventSchemaTypedDict]:
    """Crawl Ibiza Spotlight listing page and scrape events."""
    
    print(f"[INFO] Starting crawl of {listing_url}")
    
    if sync_playwright is None:
        print("[ERROR] Playwright is required for Ibiza Spotlight scraping")
        return []
    
    events = []
    scraper = MultiLayerEventScraper(use_browser=True, headless=headless)
    
    # Fetch the listing page
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(
            user_agent=random.choice(MODERN_USER_AGENTS),
            viewport={'width': 1920, 'height': 1080}
        )
        page = context.new_page()
        
        try:
            print(f"[INFO] Fetching listing page: {listing_url}")
            page.goto(listing_url, timeout=60000)  # Increased timeout to 60 seconds
            
            # Handle cookie consent
            try:
                cookie_button = page.locator('text="NO PROBLEM"').first
                if cookie_button.is_visible(timeout=3000):
                    cookie_button.click()
                    time.sleep(1)
            except:
                pass
            
            # Wait for content
            page.wait_for_load_state('networkidle')
            
            # Scroll to load more content
            for _ in range(3):
                page.evaluate("window.scrollBy(0, window.innerHeight)")
                time.sleep(1)
            
            html = page.content()
            
            # Extract event links
            base_url = "https://www.ibiza-spotlight.com"
            event_links = extract_ibiza_spotlight_event_links(html, base_url)
            
            print(f"[INFO] Found {len(event_links)} event links")
            
            # Limit to max_events
            event_links = event_links[:max_events]
            
            browser.close()
            
            # Scrape each event
            for idx, event_url in enumerate(event_links, 1):
                print(f"\n[INFO] Scraping event {idx}/{len(event_links)}: {event_url}")
                
                try:
                    event_data = scraper.scrape_event_strategically(event_url)
                    
                    if event_data:
                        events.append(event_data)
                        print(f"[SUCCESS] Scraped: {event_data.get('title', 'Unknown')}")
                    else:
                        print(f"[WARNING] No data extracted for {event_url}")
                    
                    # Rate limiting
                    if idx < len(event_links):
                        delay = random.uniform(2.0, 4.0)
                        print(f"[INFO] Waiting {delay:.1f}s before next request...")
                        time.sleep(delay)
                        
                except Exception as e:
                    print(f"[ERROR] Failed to scrape {event_url}: {e}")
                    continue
                    
        except Exception as e:
            print(f"[ERROR] Failed to fetch listing page: {e}")
            browser.close()
            return []
    
    return events


def datetime_serializer(obj):
    """JSON serializer for datetime objects."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def format_event_to_markdown(event_data: EventSchemaTypedDict) -> str:
    """Format event data to markdown."""
    md_lines = []
    
    # Title
    title = event_data.get("title", "Unknown Event")
    md_lines.append(f"# {title}\n")
    
    # Basic Info
    md_lines.append("## Basic Information\n")
    
    # Date/Time
    date_info = event_data.get("dateTime", {})
    if date_info:
        display_text = date_info.get("displayText", "")
        parsed = date_info.get("parsed", {})
        if parsed and parsed.get("startDate"):
            start_date = parsed["startDate"]
            if isinstance(start_date, str):
                md_lines.append(f"- **Date**: {start_date}")
            else:
                md_lines.append(f"- **Date**: {start_date.strftime('%A, %B %d, %Y')}")
            if parsed.get("doors"):
                md_lines.append(f"- **Time**: {parsed['doors']}")
        elif display_text:
            md_lines.append(f"- **Date/Time**: {display_text}")
    
    # Venue
    location = event_data.get("location", {})
    if location and location.get("venue"):
        md_lines.append(f"- **Venue**: {location['venue']}")
        if location.get("address"):
            md_lines.append(f"- **Address**: {location['address']}")
    
    # Artists
    lineup = event_data.get("lineUp", [])
    if lineup:
        md_lines.append("\n## Line Up\n")
        for artist in lineup:
            name = artist.get("name", "")
            if artist.get("headliner"):
                md_lines.append(f"- **{name}** (Headliner)")
            else:
                md_lines.append(f"- {name}")
    
    # Ticket Info
    ticket_info = event_data.get("ticketInfo", {})
    if ticket_info:
        md_lines.append("\n## Ticket Information\n")
        
        if ticket_info.get("tiers"):
            for tier in ticket_info["tiers"]:
                tier_name = tier.get("name", "Ticket")
                price = tier.get("price")
                if price is not None:
                    md_lines.append(f"- **{tier_name}**: {price}€")
                else:
                    md_lines.append(f"- **{tier_name}**")
        elif ticket_info.get("startingPrice"):
            md_lines.append(f"- **Starting Price**: {ticket_info['startingPrice']}€")
        
        if ticket_info.get("displayText"):
            md_lines.append(f"- **Info**: {ticket_info['displayText']}")
    
    # Description
    if event_data.get("fullDescription"):
        md_lines.append("\n## Description\n")
        md_lines.append(event_data["fullDescription"])
    
    # Metadata
    md_lines.append("\n## Metadata\n")
    md_lines.append(f"- **URL**: {event_data.get('url', '')}")
    md_lines.append(f"- **Extraction Method**: {event_data.get('extractionMethod', 'unknown')}")
    md_lines.append(f"- **Scraped At**: {event_data.get('scrapedAt', '')}")
    
    if event_data.get("hasTicketInfo"):
        md_lines.append(f"- **Has Ticket Info**: Yes")
    if event_data.get("isFree"):
        md_lines.append(f"- **Free Event**: Yes")
    if event_data.get("isSoldOut"):
        md_lines.append(f"- **Sold Out**: Yes")
    
    md_lines.append("\n---\n")
    
    return "\n".join(md_lines)


def main():
    parser = argparse.ArgumentParser(
        description="Scrape events from Ibiza Spotlight"
    )
    parser.add_argument(
        "action",
        choices=["scrape", "crawl"],
        help="Action to perform: 'scrape' a single URL or 'crawl' a listing page",
    )
    parser.add_argument(
        "--url",
        default=DEFAULT_TARGET_URL,
        help=f"Target URL (default: {DEFAULT_TARGET_URL})",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=5,
        help="Maximum number of events to scrape when crawling (default: 5)",
    )
    parser.add_argument(
        "--output",
        choices=["json", "markdown", "both"],
        default="both",
        help="Output format (default: both)",
    )
    parser.add_argument(
        "--output-dir",
        default="output/ibiza_spotlight",
        help="Output directory (default: output/ibiza_spotlight)",
    )
    parser.add_argument(
        "--no-headless",
        action="store_true",
        help="Run browser in non-headless mode",
    )

    args = parser.parse_args()
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if args.action == "scrape":
        # Single event scraping
        print(f"Scraping single event: {args.url}")
        scraper = MultiLayerEventScraper(
            use_browser=True,
            headless=not args.no_headless
        )
        
        event_data = scraper.scrape_event_strategically(args.url)
        
        if event_data:
            # Save JSON
            if args.output in ["json", "both"]:
                json_path = output_dir / f"event_{timestamp}.json"
                with json_path.open("w", encoding="utf-8") as f:
                    json.dump(event_data, f, indent=2, ensure_ascii=False, default=datetime_serializer)
                print(f"Saved JSON to: {json_path}")
            
            # Save Markdown
            if args.output in ["markdown", "both"]:
                md_content = format_event_to_markdown(event_data)
                md_path = output_dir / f"event_{timestamp}.md"
                with md_path.open("w", encoding="utf-8") as f:
                    f.write(md_content)
                print(f"Saved Markdown to: {md_path}")
        else:
            print("No data extracted")
    
    else:  # crawl
        # Crawl listing page
        print(f"Crawling listing page: {args.url}")
        print(f"Maximum events to scrape: {args.max_events}")
        
        events = crawl_ibiza_spotlight_events(
            args.url,
            max_events=args.max_events,
            headless=not args.no_headless,
            output_format=args.output
        )
        
        if events:
            print(f"\nSuccessfully scraped {len(events)} events")
            
            # Save all events JSON
            if args.output in ["json", "both"]:
                json_path = output_dir / f"events_{timestamp}.json"
                with json_path.open("w", encoding="utf-8") as f:
                    json.dump(events, f, indent=2, ensure_ascii=False, default=datetime_serializer)
                print(f"Saved JSON to: {json_path}")
            
            # Save all events Markdown
            if args.output in ["markdown", "both"]:
                md_content = ""
                for event in events:
                    md_content += format_event_to_markdown(event)
                
                md_path = output_dir / f"events_{timestamp}.md"
                with md_path.open("w", encoding="utf-8") as f:
                    f.write(md_content)
                print(f"Saved Markdown to: {md_path}")
            
            # Print summary
            print("\nEvent Summary:")
            for idx, event in enumerate(events, 1):
                title = event.get("title", "Unknown")
                venue = event.get("location", {}).get("venue", "Unknown")
                method = event.get("extractionMethod", "unknown")
                print(f"{idx}. {title} @ {venue} (via {method})")
        else:
            print("No events scraped")
    
    # Write scrape log
    log_path = output_dir / "scrape_log.md"
    with log_path.open("a", encoding="utf-8") as f:
        f.write(f"\n## Scrape Session - {timestamp}\n")
        f.write(f"- **Target URL**: {args.url}\n")
        f.write(f"- **Action**: {args.action}\n")
        if args.action == "crawl":
            f.write(f"- **Events Scraped**: {len(events) if 'events' in locals() else 0}\n")
        f.write(f"- **Output Format**: {args.output}\n")
        f.write(f"- **Headless**: {not args.no_headless}\n")
        f.write("\n")


if __name__ == "__main__":
    main()