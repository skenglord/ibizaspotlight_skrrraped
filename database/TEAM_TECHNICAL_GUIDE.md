# 🎉 Easy Guide: Tickets Ibiza Event Data API for Your App!

Welcome, Team! This guide will help you understand and use the new Tickets Ibiza Event Data API. It's designed to give your app the best, most reliable event information.

---

## What is this API? 🤔

Think of this API as a smart librarian for all our Tickets Ibiza event data.

*   **It collects data**: Our special "scraper" program goes to the Tickets Ibiza website and gathers all event details.
*   **It checks quality**: Every piece of information (like event name, location, date) gets a "quality score" to make sure it's accurate and complete.
*   **It stores everything**: All this data is organized neatly in a database called MongoDB.
*   **It serves it to you**: The API lets your app easily ask for and receive this high-quality event data.

**Why is this important?** We want to show our users the most accurate and up-to-date event information possible, and this system helps us do that automatically!

---

## Getting Started: How to Use the API 🚀

Before your app can talk to our "smart librarian" (the API), we need to do a few quick setups.

### Step 1: Get Ready (Prerequisites)

Make sure you have these tools installed on your computer:

*   **Python**: Version 3.9 or newer.
*   **MongoDB**: This is our database. If you don't have it, follow the simple instructions here: [MongoDB Installation Guide](https://www.mongodb.com/docs/manual/installation/).
    *   **Pro Tip for Docker Users**: If you use Docker, you can run MongoDB very easily with this command:
        ```bash
        docker run -d -p 27017:27017 --name my-mongodb mongo:latest
        ```
*   **Python Libraries**: Open your terminal or command prompt, navigate to the `skrrraped_graph/database` folder, and run this command:
    ```bash
    cd skrrraped_graph/database
    pip install -r requirements.txt
    ```
    This installs all the necessary "ingredients" for our Python code.

### Step 2: Start the API Server (Your Librarian at Work!)

The API server is the program that listens for requests from your app and sends back event data.

1.  Open your terminal or command prompt.
2.  Navigate to the `skrrraped_graph/database` folder:
    ```bash
    cd skrrraped_graph/database
    ```
3.  Run the API server:
    ```bash
    python api_server.py
    ```
    You should see messages indicating the server is starting. It will usually run on `http://localhost:8000`.

**Important**: Keep this terminal window open while you are using the API, as the server needs to keep running!

### Step 3: Explore the API (Meet the Librarian!)

Once the server is running, you can visit a special page in your web browser to see all the available API "requests" or "endpoints."

Open your web browser and go to:
[`http://localhost:8000/docs`](http://localhost:8000/docs)

This page is like a menu for our API. You can click on different sections (like "Events" or "Venues") to see what kind of information you can ask for and what options you have.

---

## How to Get Event Data for Your App (Making Requests)

Now that the API is running, let's see how your app can ask for event data. We'll use Python examples, but the same idea applies to JavaScript, PHP, or any other language your app uses.

Our API "speaks" JSON, which is a common way for computers to exchange data.

### Example 1: Get All Good Quality Upcoming Events

This is probably the most common request! We want events that are happening in the future and have a good quality score.

**API Endpoint**: `GET /api/events`

**What it means**: "GET me some events from the API."

**Options (Parameters)**:

*   `min_quality`: How good does the data need to be? (0.0 to 1.0, 0.7 is default, 0.8 is "good")
*   `future_only`: Only show events that haven't happened yet? (`true` or `false`)
*   `limit`: How many events do you want back? (e.g., `20`, `50`)
*   `skip`: If you want to skip the first few results (for pagination, e.g., `skip=50` to get the next 50 events)

**Python Code Example**:

```python
import requests

# The address of our API server
base_url = "http://localhost:8000"

# What we want to ask for: events!
endpoint = "/api/events"

# Our specific requests:
params = {
    "min_quality": 0.8,    # Only events with a quality score of 0.8 or higher
    "future_only": True,   # Only events happening from now on
    "limit": 50,           # Give me up to 50 events
    "skip": 0              # Start from the beginning
}

# Send the request!
response = requests.get(f"{base_url}{endpoint}", params=params)

# Check if the request was successful
if response.status_code == 200:
    events = response.json()
    print(f"Found {len(events)} high-quality upcoming events:")
    for event in events:
        print(f"- {event['title']} at {event['venue']} (Quality: {event['qualityScore']:.2f})")
else:
    print(f"Error getting events: {response.status_code} - {response.text}")
```

### Example 2: Search for Events (e.g., "Techno" Music)

Want to find events related to a specific keyword?

**API Endpoint**: `GET /api/events/search/{your_search_term}`

**What it means**: "GET me events that match my search term."

**Options (Parameters)**:

*   `min_quality` (float, optional): Minimum quality score for search results.
*   `limit` (int, optional): Maximum number of results.

**Python Code Example**:

```python
import requests

base_url = "http://localhost:8000"
search_term = "techno" # What you're looking for

params = {
    "min_quality": 0.7, # Only show results with at least 0.7 quality
    "limit": 10         # Get up to 10 results
}

response = requests.get(f"{base_url}/api/events/search/{search_term}", params=params)

if response.status_code == 200:
    results = response.json()
    print(f"\nFound {len(results)} events related to '{search_term}':")
    for event in results:
        print(f"- {event['title']} at {event['venue']} (Quality: {event['qualityScore']:.2f})")
else:
    print(f"Error searching: {response.status_code} - {response.text}")
```

### Example 3: Get Events at a Specific Venue (e.g., "Hï Ibiza")

**API Endpoint**: `GET /api/venues/{venue_name}/events`

**What it means**: "GET me events happening at a specific venue."

**Options (Parameters)**:

*   `future_only` (boolean): Show only future events.
*   `limit` (int): Maximum number of results.

**Python Code Example**:

```python
import requests

base_url = "http://localhost:8000"
venue_name = "Hï Ibiza" # The venue you're interested in

params = {
    "future_only": True, # Only upcoming events at this venue
    "limit": 50          # Get up to 50 events
}

response = requests.get(f"{base_url}/api/venues/{venue_name}/events", params=params)

if response.status_code == 200:
    venue_events = response.json()
    print(f"\nFound {len(venue_events)} events at {venue_name}:")
    for event in venue_events:
        print(f"- {event['title']} on {event['date']} (Quality: {event['qualityScore']:.2f})")
else:
    print(f"Error getting venue events: {response.status_code} - {response.text}")
```

### Example 4: Get Overall Quality Statistics

Want to see how good our data is overall?

**API Endpoint**: `GET /api/stats/quality`

**What it means**: "GET me a summary of our data quality."

**Python Code Example**:

```python
import requests

base_url = "http://localhost:8000"

response = requests.get(f"{base_url}/api/stats/quality")

if response.status_code == 200:
    stats = response.json()
    print("\n--- Data Quality Statistics ---")
    print(f"Total Events: {stats['totalEvents']}")
    print(f"Average Quality Score: {stats['averageQuality']:.3f}")
    print("\nQuality Distribution:")
    for level, count in stats['distribution'].items():
        print(f"  {level.capitalize()}: {count} events")
    print("\nTop Venues by Average Quality:")
    for venue_data in stats['topVenues']:
        print(f"  - {venue_data['venue']}: {venue_data['avgQuality']:.3f} (Events: {venue_data['eventCount']})")
else:
    print(f"Error getting statistics: {response.status_code} - {response.text}")
```

### Example 5: Get Upcoming Events for Next X Days

Need events happening in the next week, 2 weeks, or month?

**API Endpoint**: `GET /api/upcoming`

**Options (Parameters)**:

*   `days` (int, optional): Number of days ahead to look (e.g., `7` for next week).
*   `min_quality` (float, optional): Minimum quality score.
*   `limit` (int, optional): Maximum number of results.

**Python Code Example**:

```python
import requests

base_url = "http://localhost:8000"

params = {
    "days": 14,          # Look for events in the next 14 days
    "min_quality": 0.75, # Only events with at least 0.75 quality
    "limit": 30          # Get up to 30 events
}

response = requests.get(f"{base_url}/api/upcoming", params=params)

if response.status_code == 200:
    upcoming_events = response.json()
    print(f"\nFound {len(upcoming_events)} upcoming events in the next 14 days:")
    for event in upcoming_events:
        print(f"- {event['title']} on {event['date']} at {event['venue']}")
else:
    print(f"Error getting upcoming events: {response.status_code} - {response.text}")
```

---

## Understanding Quality Scores 📊

Every event in our system has a `qualityScore`. This is a number between 0.0 (very bad) and 1.0 (perfect).

*   **How it's calculated**: We look at how complete and accurate different parts of the event data are (like the event name, location, date, lineup, and ticket info). Each part gets a score, and then we combine them for an overall score.
*   **What the scores mean**:
    *   **Excellent (0.9 - 1.0)**: Super reliable data!
    *   **Good (0.8 - 0.9)**: Very good, safe to use.
    *   **Fair (0.7 - 0.8)**: Decent, might have minor missing details.
    *   **Poor (< 0.7)**: Missing important info, use with caution.

**Your app should prioritize events with higher quality scores!** This ensures our users always see the best information.

---

## Exporting Event Data to Google Sheets 📈

Sometimes, you might want to get event data directly into a Google Sheet for analysis, reporting, or sharing. Here's how you can do it using Python.

### Step 1: Set Up Google Sheets API Access

This is the most complex part, but you only need to do it once per project.

1.  **Enable the Google Sheets API**:
    *   Go to the [Google Cloud Console](https://console.cloud.google.com/).
    *   Create a new project or select an existing one.
    *   In the search bar, type "Google Sheets API" and select it.
    *   Click "Enable."
2.  **Create Service Account Credentials**:
    *   In the Google Cloud Console, go to "APIs & Services" > "Credentials."
    *   Click "Create Credentials" > "Service Account."
    *   Give it a name (e.g., `sheets-writer`) and click "Create and Continue."
    *   Grant it the "Editor" role (or a more specific role if you know it).
    *   Click "Done."
    *   Click on the newly created service account. Go to the "Keys" tab.
    *   Click "Add Key" > "Create new key" > "JSON."
    *   A JSON file will download. **Rename this file to `credentials.json`** and save it in your `skrrraped_graph/database` folder (or a secure location). **Keep this file private!**
3.  **Share Your Google Sheet with the Service Account**:
    *   Open the Google Sheet you want to write to.
    *   Click the "Share" button.
    *   In the "Share with people and groups" box, paste the `client_email` from your `credentials.json` file (it looks like an email address, e.g., `your-service-account@your-project-id.iam.gserviceaccount.com`).
    *   Grant it "Editor" access.
    *   Click "Share."

### Step 2: Install Google API Client Library

Open your terminal in the `skrrraped_graph/database` folder and install the necessary Python library:

```bash
pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib pandas
```

### Step 3: Write the Python Script

Create a new Python file (e.g., `export_to_sheets.py`) in your `skrrraped_graph/database` folder.

```python
import requests
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
import json
from datetime import datetime

# --- Configuration ---
API_BASE_URL = "http://localhost:8000"
GOOGLE_SHEET_ID = "YOUR_GOOGLE_SHEET_ID_HERE" # Replace with your Sheet ID
GOOGLE_CREDENTIALS_FILE = "credentials.json" # Make sure this file is in the same directory

# --- Google Sheets API Setup ---
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
creds = service_account.Credentials.from_service_account_file(
    GOOGLE_CREDENTIALS_FILE, scopes=SCOPES)
service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()

# --- Function to fetch events from our API ---
def fetch_events_from_api(min_quality=0.7, future_only=True, limit=100):
    endpoint = "/api/events"
    params = {
        "min_quality": min_quality,
        "future_only": future_only,
        "limit": limit
    }
    try:
        response = requests.get(f"{API_BASE_URL}{endpoint}", params=params)
        response.raise_for_status() # Raise an error for bad status codes
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching events from API: {e}")
        return []

# --- Function to prepare data for Google Sheets ---
def prepare_data_for_sheets(events_data):
    if not events_data:
        return pd.DataFrame() # Return empty DataFrame if no data

    # Normalize the data: flatten nested dictionaries for easier DataFrame creation
    # We'll select key fields for our sheet
    flattened_data = []
    for event in events_data:
        flat_event = {
            "ID": event.get('id'),
            "Title": event.get('title'),
            "URL": event.get('url'),
            "Venue": event.get('venue'),
            "Date Display": event.get('date'),
            "Quality Score": event.get('qualityScore'),
            "Ticket Status": event.get('status')
            # Add more fields as needed, e.g., 'fullDescription', 'images'
        }
        flattened_data.append(flat_event)
    
    df = pd.DataFrame(flattened_data)
    return df

# --- Function to write DataFrame to Google Sheet ---
def write_dataframe_to_sheet(dataframe, sheet_id, range_name="Sheet1!A1"):
    if dataframe.empty:
        print("No data to write to Google Sheet.")
        return

    # Prepare data for Sheets API
    # Convert DataFrame to list of lists (header + data)
    header = dataframe.columns.tolist()
    values = dataframe.values.tolist()
    body = {
        'values': [header] + values
    }

    try:
        # Clear existing data in the range first if you want to overwrite
        # sheet.values().clear(spreadsheetId=sheet_id, range=range_name).execute()
        
        result = sheet.values().update(
            spreadsheetId=sheet_id, 
            range=range_name,
            valueInputOption="RAW",
            body=body).execute()
        print(f"Data successfully written to Google Sheet. Cells updated: {result.get('updatedCells')}")
    except Exception as e:
        print(f"Error writing to Google Sheet: {e}")

# --- Main execution ---
if __name__ == "__main__":
    # Make sure your API server is running (http://localhost:8000)
    
    print("Fetching events from API...")
    # Fetch events with a minimum quality of 0.75 and up to 200 events
    events_to_export = fetch_events_from_api(min_quality=0.75, limit=200)
    
    if events_to_export:
        print(f"Preparing {len(events_to_export)} events for Google Sheet...")
        df_events = prepare_data_for_sheets(events_to_export)
        
        print("Writing data to Google Sheet...")
        # You can specify a different sheet name or starting cell, e.g., "Events!A1"
        write_dataframe_to_sheet(df_events, GOOGLE_SHEET_ID, range_name="EventsData!A1")
        print("\nExport process completed.")
    else:
        print("No events fetched from API to export.")

```

### Step 4: Run the Export Script

1.  **Replace `YOUR_GOOGLE_SHEET_ID_HERE`**: In `export_to_sheets.py`, find this placeholder and replace it with the actual ID of your Google Sheet. You can find the Sheet ID in the URL of your Google Sheet:
    `https://docs.google.com/spreadsheets/d/YOUR_GOOGLE_SHEET_ID_HERE/edit`
2.  **Ensure API Server is Running**: Make sure `api_server.py` is running in another terminal window.
3.  **Run the script**: Open your terminal in the `skrrraped_graph/database` folder and run:
    ```bash
    python export_to_sheets.py
    ```

You should see messages indicating data is being fetched and then written to your Google Sheet!

---

## Need More Details? (Advanced Usage)

### Getting a Single Event's Full Details

If you have an event's unique `id` (you'll get this from the `/api/events` list), you can ask for all its details, including the specific quality scores for each field.

**API Endpoint**: `GET /api/events/{event_id}`

**Python Code Example**:

```python
import requests

base_url = "http://localhost:8000"
event_id = "60c72b2f9f1b2c001c8e4d5f" # Replace with an actual event ID from your API call

response = requests.get(f"{base_url}/api/events/{event_id}")

if response.status_code == 200:
    event_details = response.json()
    print(f"\n--- Details for {event_details['title']} ---")
    print(f"Full Description: {event_details.get('fullDescription', 'N/A')[:200]}...")
    print(f"Overall Quality: {event_details['qualityScore']['overall']:.2f}")
    print(f"Location Quality: {event_details['qualityScore']['location']:.2f}")
    # You can access all fields here
else:
    print(f"Error getting event details: {response.status_code} - {response.text}")
```

### Refreshing Event Data (for Scraper Team)

If you notice an event's data is wrong or outdated, you can tell the system to re-scrape it.

**API Endpoint**: `POST /api/events/{event_id}/refresh`

This tells our scraper to get fresh data for that specific event.

**Python Code Example**:

```python
import requests

base_url = "http://localhost:8000"
event_id_to_refresh = "60c72b2f9f1b2c001c8e4d5f" # Replace with the ID of the event you want to refresh

response = requests.post(f"{base_url}/api/events/{event_id_to_refresh}/refresh")

if response.status_code == 200:
    print(f"\nSuccessfully requested refresh for event ID: {event_id_to_refresh}")
else:
    print(f"Error requesting refresh: {response.status_code} - {response.text}")
```

---

## Troubleshooting Tips 💡

If something isn't working, don't panic! Here are common issues and solutions:

1.  **"Connection Refused" Error**:
    *   **Problem**: Your app can't connect to the API server.
    *   **Solution**: Make sure the API server (from "Step 2: Start the API Server" above) is actually running in a terminal window. Also, check if MongoDB is running (`mongod` command).
2.  **No Events Returned**:
    *   **Problem**: The API is running, but you're not getting any event data back.
    *   **Solution**:
        *   Check your `min_quality` parameter. If it's too high, you might be filtering out all events. Try lowering it.
        *   Check your `future_only` parameter. If it's `true` and all events are in the past, you'll get nothing.
        *   Make sure there's actually data in the database (you can check using `python skrrraped_graph/database/query_examples.py` to see if it prints events).
3.  **"Error 404: Not Found"**:
    *   **Problem**: You're asking for an API address that doesn't exist.
    *   **Solution**: Double-check the endpoint URL you are using. Are there any typos?
4.  **Google Sheets API Errors**:
    *   **Problem**: Issues with `export_to_sheets.py`.
    *   **Solution**:
        *   Double-check that `credentials.json` is correctly placed and named.
        *   Verify that you have shared your Google Sheet with the service account email from `credentials.json`.
        *   Ensure `YOUR_GOOGLE_SHEET_ID_HERE` is replaced with the correct ID.
        *   Check your internet connection.

---

## Questions? 🙋‍♀️🙋‍♂️

If you have any questions or run into problems, please don't hesitate to reach out to the development team. We're here to help!

**Contact**: [Your Team's Support Contact, e.g., #dev-support Slack channel]
**Last Updated**: 2025-05-26