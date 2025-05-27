import asyncio
from playwright.async_api import async_playwright
import re
import json
from datetime import datetime

# Event Schema for MongoDB
event_schema = {
    "title": str,
    "url": str,
    "location": {
        "venue": str,
        "address": str,
        "coordinates": {
            "lat": float,
            "lng": float
        }
    },
    "dateTime": {
        "displayText": str,
        "parsed": {
            "startDate": datetime,
            "endDate": datetime,
            "doors": str
        },
        "dayOfWeek": str
    },
    "lineUp": [{
        "name": str,
        "setInfo": str,
        "setDuration": str,
        "setType": str,
        "affiliates": [str],
        "genres": [str],
        "headliner": bool
    }],
    "eventType": [str],
    "genres": [str],
    "ticketInfo": {
        "displayText": str,
        "startingPrice": float,
        "currency": str,
        "tiers": [{
            "name": str,
            "price": float,
            "available": bool
        }],
        "status": str,
        "url": str
    },
    "promos": [str],
    "organizer": {
        "name": str,
        "affiliates": [str],
        "socialLinks": object
    },
    "ageRestriction": str,
    "images": [str],
    "socialLinks": object,
    "fullDescription": str,
    "hasTicketInfo": bool,
    "isFree": bool,
    "isSoldOut": bool,
    "artistCount": int,
    "imageCount": int,
    "scrapedAt": datetime,
    "updatedAt": datetime,
    "lastCheckedAt": datetime
}


async def scrape_event_data(page, url):
    """Scrape event data from a single event page."""
    try:
        await page.goto(url, timeout=30000)
        await page.wait_for_selector('body', timeout=30000)
        html_content = await page.content()
        title = await page.locator('h1.post-title', timeout=30000).inner_text()

        event_data = {
            "title": title,
            "url": url,
            "html": html_content,
            "scrapedAt": datetime.utcnow().isoformat() + "Z",
            "updatedAt": datetime.utcnow().isoformat() + "Z",
            "lastCheckedAt": datetime.utcnow().isoformat() + "Z",
        }
        return event_data
    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return None


async def main():
    """Main function to orchestrate the scraping process."""
    output_data = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  # Set headless to False
        page = await browser.new_page()
        await page.goto("https://ticketsibiza.com/ibiza-calendar/2025-events/")
        await page.wait_for_selector('body')
        await page.wait_for_timeout(2000)  # Wait for 2 seconds

        event_urls = []
        info_links = await page.locator('a:has-text("Info")').all()

        for link in info_links:
            url = await link.get_attribute('href')
            event_urls.append(url)

        for event_url in event_urls:
            event_data = await scrape_event_data(page, event_url)
            if event_data:
                output_data.append(event_data)

        await browser.close()

    # Output structured markdown data
    markdown_output = ""
    for event in output_data:
        markdown_output += f"## {event['title']}\\n"
        markdown_output += f"- URL: {event['url']}\\n"
        markdown_output += f"- Scraped At: {event['scrapedAt']}\\n\\n"

    # Save to markdown file
    output_file = "pw_mistune_data_run1.md"
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(markdown_output)

    print(f"Scraped data saved to {output_file}")


if __name__ == "__main__":
    asyncio.run(main())