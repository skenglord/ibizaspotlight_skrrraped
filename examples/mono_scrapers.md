# Mono Scrapers

This repository provides a couple of self contained "mono" scrapers that do not rely on the Scrapegraph-ai graphs. These scripts are located in the project root and can be executed directly from the command line.

## `mono_ticketmaster.py`

A single file scraper for Ticketmaster style event pages. It can optionally crawl a listing page and uses `requests` and `BeautifulSoup` for fetching and parsing. HTML is converted to Markdown via `mistune` and optionally `pypandoc`. Playwright is used only when the `--no-browser` flag is not set.

### Dependencies

- Python 3.8+
- `requests`
- `beautifulsoup4`
- `mistune`
- `pypandoc` *(optional, for improved Markdown conversion)*
- `playwright` *(optional, required for browser based scraping)*

Install all required packages with:

```bash
pip install -r requirements.txt
```

### Usage

```bash
# Scrape a single event
python mono_ticketmaster.py --target-url https://example.com/event

# Crawl the default listing and follow each INFO link
python mono_ticketmaster.py --crawl-listing
```

The scraped data is written to `ticketsibiza_scraped_data.json` and a Markdown summary is saved to `ticketsibiza_event_data_parsed.md`.

## `playwright_mistune_scraper.py`

A minimal asynchronous scraper that demonstrates how to gather event information using Playwright. Scraped pages are stored in Markdown format using `mistune`.

### Dependencies

- Python 3.8+
- `playwright`
- `mistune`

Install Playwright and its browser drivers first:

```bash
pip install playwright mistune
playwright install
```

### Usage

```bash
python playwright_mistune_scraper.py
```

Results are saved to `pw_mistune_data_run1.md`.
