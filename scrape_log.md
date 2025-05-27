# Scrape Log

## Latest Scrape Details

- **Date & Time:** 2025-05-26, 11:17:05 AM (UTC+7)
- **Target URL:** https://ticketsibiza.com/ibiza-calendar/2025-events/
- **Total Events Scraped:** 968
- **Output Files:**
    - `ticketsibiza_scraped_data.json`: 7.8M
    - `ticketsibiza_event_data_parsed.md`: 1.8M
- **Extraction Method Breakdown:**
    - `jsonld`: 967 events (primary method, highly successful)
    - `fallback`: 1 event (used for information pages or where JSON-LD was absent)
- **Navigation Method:** The script uses `crawl_listing_for_events` to navigate through the listing page, extracting individual event URLs and then scraping each one. It prioritizes direct requests for speed and falls back to Playwright (browser) only when necessary.
- **Stealth Used/Needed:** User-Agent rotation was employed during the crawl to mimic different browsers and reduce the likelihood of detection. No explicit proxy was used during this run, but the script is designed to integrate with proxies if needed.
- **Headless/Non-Headless:** The scraping was performed in **headless** mode for efficiency (`--headless` was the default for the `crawl_listing_for_events` test).
- **Proxy Used:** No proxy was explicitly configured or used during this specific run.
- **Summary:** The script successfully crawled and extracted a large volume of event data from the TicketsIbiza website, demonstrating robust multi-layer extraction capabilities and efficient handling of various page structures.