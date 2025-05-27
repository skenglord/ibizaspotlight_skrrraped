# Ticketmaster Scraper Config Tutorial

This guide explains how to run the `ticketmaster_scraper.py` example for collecting event information from Ticketmaster style pages.

## Setup
1. Install the Python requirements and Playwright drivers:
   ```bash
   pip install -r requirements.txt
   playwright install
   ```
2. Ensure the project root contains `ticketsibiza_event_data.json` with the list of event URLs to scrape.

## Required Files
- `examples/smart_scraper_graph/ticketmaster_scraper.py` – main scraper script.
- `ticketsibiza_event_data.json` – JSON file containing the event URLs.
- `run_fetch_sessions.sh` – optional helper script that runs multiple scrape sessions.

## Example Commands
Run the scraper using the default events file:
```bash
python examples/smart_scraper_graph/ticketmaster_scraper.py
```
Provide a custom list of URLs:
```bash
python examples/smart_scraper_graph/ticketmaster_scraper.py path/to/events.json
```
The script outputs `ticketsibiza_scraped_data.json` and `ticketsibiza_event_data_parsed.md` in the repository root.

### Using the helper script
The `run_fetch_sessions.sh` script automates multiple runs and saves results under `output/`:
```bash
chmod +x run_fetch_sessions.sh
./run_fetch_sessions.sh
```
