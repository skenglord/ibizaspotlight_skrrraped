#!/usr/bin/env python3

import sys
import time
from pathlib import Path
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# Add the current directory to sys.path
sys.path.insert(0, str(Path(__file__).parent))

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("Playwright not available")
    sys.exit(1)

def debug_ibiza_links():
    """Debug script to see what links are available on Ibiza Spotlight"""
    
    url = "https://www.ibiza-spotlight.com/night/events"
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)  # Non-headless for debugging
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={'width': 1920, 'height': 1080}
        )
        page = context.new_page()
        
        try:
            print(f"Navigating to: {url}")
            page.goto(url, timeout=60000)
            
            # Handle cookie consent
            try:
                cookie_button = page.locator('text="NO PROBLEM"').first
                if cookie_button.is_visible(timeout=5000):
                    print("Accepting cookies...")
                    cookie_button.click()
                    time.sleep(2)
            except:
                print("No cookie banner or already accepted")
            
            # Wait for content
            page.wait_for_load_state('networkidle')
            
            # Scroll to load more content
            print("Scrolling to load content...")
            for i in range(3):
                page.evaluate("window.scrollBy(0, window.innerHeight)")
                time.sleep(1)
            
            html = page.content()
            soup = BeautifulSoup(html, "html.parser")
            
            print(f"\nPage title: {soup.title.string if soup.title else 'No title'}")
            
            # Look for all links
            all_links = soup.find_all('a', href=True)
            print(f"\nTotal links found: {len(all_links)}")
            
            # Filter for event-related links
            event_links = []
            for link in all_links:
                href = link.get('href')
                if href and '/night/events/' in href:
                    full_url = urljoin(url, href)
                    text = link.get_text(strip=True)
                    event_links.append((full_url, text[:100]))
            
            print(f"\nEvent links found: {len(event_links)}")
            for i, (link, text) in enumerate(event_links[:10]):  # Show first 10
                print(f"{i+1}. {link}")
                print(f"   Text: {text}")
                print()
            
            # Look for specific patterns
            print("\n=== Debugging specific selectors ===")
            
            # Test our current selectors
            selectors_to_test = [
                "a[href*='/night/events/']",
                ".event-card a",
                ".event-listing a", 
                "h3 a",
                "h4 a",
                "a:contains('presents')"
            ]
            
            for selector in selectors_to_test:
                try:
                    elements = soup.select(selector)
                    print(f"Selector '{selector}': {len(elements)} matches")
                    for elem in elements[:3]:  # Show first 3
                        href = elem.get('href')
                        text = elem.get_text(strip=True)[:50]
                        print(f"  - {href} | {text}")
                except Exception as e:
                    print(f"Selector '{selector}': Error - {e}")
            
            # Look for event-related text patterns
            print("\n=== Looking for event text patterns ===")
            text_content = soup.get_text()
            
            patterns = ['presents', 'opening', 'closing', 'party', 'club', 'dj']
            for pattern in patterns:
                count = text_content.lower().count(pattern)
                print(f"Pattern '{pattern}': {count} occurrences")
            
            # Save HTML for manual inspection
            with open('debug_ibiza_page.html', 'w', encoding='utf-8') as f:
                f.write(html)
            print(f"\nSaved page HTML to debug_ibiza_page.html")
            
        except Exception as e:
            print(f"Error: {e}")
        finally:
            browser.close()

if __name__ == "__main__":
    debug_ibiza_links()