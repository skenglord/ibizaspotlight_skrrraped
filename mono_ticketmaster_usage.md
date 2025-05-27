# mono_ticketmaster.py Usage Guide

This guide explains how to run the monolithic Ticketmaster scraper `mono_ticketmaster.py` and details the required dependencies.

## Installation

1. Install Python 3.8 or newer.
2. Install the dependencies listed in `requirements.txt`:
   ```bash
   pip install -r requirements.txt
   ```
   `mono_ticketmaster.py` mainly relies on `requests`, `beautifulsoup4`, `mistune` and optionally `pypandoc` for HTML to Markdown conversion.
   Installing from `requirements.txt` ensures all needed packages are available.

## Configuration

- You can provide a direct event URL using the `--target-url` option. This is the
  preferred way to scrape a single page. When crawling a listing, this option is
  used as the starting URL and defaults to
  `https://ticketsibiza.com/ibiza-calendar/2025-events/`.
- If no target URL is supplied, the scraper falls back to a JSON file containing
  pre-scraped event URLs. By default it looks for `ticketsibiza_event_data.json`
  in the repository root. Each item in this file should be an object with a
  `url` key.
- To use a different file, provide the path as a positional argument.

## Running the Scraper

```bash
# scrape a single event URL (headless browser by default)
python mono_ticketmaster.py --target-url https://example.com/event

# show the browser window while scraping
python mono_ticketmaster.py --target-url https://example.com/event --show-browser

# crawl the default listing page and follow all INFO links
python mono_ticketmaster.py --crawl-listing

# fall back to scraping a list of URLs from a file
python mono_ticketmaster.py path/to/events.json
```

After scraping, two files are created in the repository root:

- `ticketsibiza_scraped_data.json` – structured data extracted from the pages.
- `ticketsibiza_event_data_parsed.md` – a Markdown summary of all events.

The script outputs progress messages and a final summary when finished.
