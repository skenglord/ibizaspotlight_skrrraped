import scrapy
import json
from datetime import datetime
import re
from scrapy_playwright.page import PageMethod
import os

class IbizaEventsSpider(scrapy.Spider):
    name = 'ibiza_events'
    allowed_domains = ['ticketsibiza.com']
    
    def start_requests(self):
        self.logger.info("Starting requests with Playwright.")
        yield scrapy.Request(
            url='https://ticketsibiza.com/ibiza-calendar/2025-events/',
            callback=self.parse,
            meta={
                'playwright': True,
                'playwright_page_methods': [
                    PageMethod('wait_for_selector', '#wcs-app-146', timeout=30000),
                    PageMethod('wait_for_function', '''
                        () => {
                            const vueApp = document.querySelector('#wcs-app-146');
                            return vueApp.__vue__ &&
                                vueApp.__vue__.$children[0] &&
                                vueApp.__vue__.$children[0].events_by_day &&
                                Object.keys(vueApp.__vue__.$children[0].events_by_day).length > 0
                        }
                    ''', timeout=30000),
                    PageMethod('wait_for_function', '''
                        () => [...document.querySelectorAll('#wcs-app-146 a.wcs-btn')]
                            .some(el => el.textContent.trim() === 'INFO')
                    ''', timeout=30000),
                    PageMethod('set_viewport_size', width=1366, height=768)
                ],
                'playwright_include_page': True,
                'playwright_page_timeout': 300000  # 5 minute timeout
            },
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
            }
        )

    def parse(self, response):
        self.logger.info(f"Parsing calendar page: {response.url}")
        
        # Save rendered HTML for debugging
        with open('rendered_calendar_page.html', 'wb') as f:
            f.write(response.body)
        self.logger.info("Saved rendered page source to rendered_calendar_page.html")
        
        # Extract INFO links
        # Extract INFO links using CSS selector and text filtering
        info_links = [a.attrib['href'] for a in response.css('a.wcs-btn') if a.css('::text').get(default='').strip() == 'INFO']
        if not info_links:
            self.logger.warning("No INFO links found on the calendar page")
            return
            
        self.logger.info(f"Found {len(info_links)} INFO links")
        
        # Follow each link to the event page
        for link in info_links:
            yield response.follow(
                link,
                callback=self.parse_event,
                meta={'playwright': True}
            )

    def parse_event(self, response):
        self.logger.info(f"Scraping event page: {response.url}")
        
        # Save raw HTML for diagnostics
        filename = f'diagnostics/event_htmls/{response.url.split("/")[-2]}.html'
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'wb') as f:
            f.write(response.body)
        self.logger.info(f"Saved HTML content to {filename}")
        
        # Initialize data dictionary
        data = {
            "title": None,
            "url": response.url,
            "location": {
                "venue": None,
                "address": None
            },
            "dateTime": {
                "displayText": None,
                "parsed": {
                    "startDate": None,
                    "endDate": None,
                    "doors": None
                },
                "dayOfWeek": None
            },
            "lineup": [],
            "eventType": [],
            "genres": [],
            "ticketInfo": {
                "displayText": None,
                "startingPrice": None,
                "currency": None,
                "tiers": [],
                "status": None,
                "url": None
            },
            "host": {
                "name": None,
                "affiliates": [],
                "socialLinks": {}
            },
            "ageRestriction": None,
            "fullDescription": None,
            "hasTicketInfo": False,
            "isFree": False,
            "isSoldOut": False,
            "artistCount": 0,
            "scrapedAt": datetime.utcnow().isoformat(),
            "updatedAt": None,
            "lastCheckedAt": datetime.utcnow().isoformat(),
            "data_attributes": {}
        }

        # Extract Title
        data['title'] = response.css('h1.tribe-events-single-event-title::text').get(default='').strip()
        
        # Extract Location
        venue = response.css('.tribe-events-venue-details .tribe-venue::text').get(default='').strip()
        address_parts = response.css('.tribe-events-venue-details address::text').getall()
        address = ", ".join([addr.strip() for addr in address_parts if addr.strip()])
        data['location']['venue'] = venue
        data['location']['address'] = address
        
        # Extract DateTime
        date_time_display = response.css('.tribe-events-schedule h2::text').get(default='').strip()
        data['dateTime']['displayText'] = date_time_display
        
        try:
            # Extract Day of Week
            date_match = re.search(r'(\w+),\s(\w+\s\d+,\s\d+)', date_time_display)
            if date_match:
                data['dateTime']['dayOfWeek'] = date_match.group(1)
                
            # Extract Doors Open time
            doors_match = re.search(r'Doors Open:\s*(\d{1,2}:\d{2}\s*[apAPmM]?)', date_time_display)
            if doors_match:
                data['dateTime']['parsed']['doors'] = doors_match.group(1)
                
        except Exception as e:
            self.logger.warning(f"Could not parse date/time from '{date_time_display}': {e}")
            pass
        
        # Extract Lineup
        lineup_elements = response.css('.tribe-events-single-event-description h3')
        for element in lineup_elements:
            artist_name = element.css('::text').get(default='').strip()
            if artist_name:
                data['lineup'].append({
                    "name": artist_name,
                    "affiliates": [],
                    "headliner": False
                })
        
        data['artistCount'] = len(data['lineup'])
        
        # Extract Event Type and Genres
        event_types = response.css('.tribe-events-event-types li::text').getall()
        data['eventType'] = [et.strip() for et in event_types if et.strip()]
        
        genres = response.css('.tribe-events-genres li::text').getall()
        data['genres'] = [g.strip() for g in genres if g.strip()]
        
        # Extract Ticket Information
        ticket_price = response.css('.tribe-events-tickets .price::text').get(default='').strip()
        if ticket_price:
            data['ticketInfo']['displayText'] = ticket_price
            # Extract price and currency
            price_match = re.search(r'(\d+[\d,.]*)\s*(\w+)', ticket_price)
            if price_match:
                data['ticketInfo']['startingPrice'] = float(price_match.group(1).replace(',', '.'))
                data['ticketInfo']['currency'] = price_match.group(2)
            # Check sold out status
            sold_out = response.css('.tribe-events-ticket-status.sold-out')
            data['isSoldOut'] = bool(sold_out)
            data['hasTicketInfo'] = True
        
        # Extract Full Description
        description = response.css('.tribe-events-single-event-description p::text').getall()
        data['fullDescription'] = ' '.join([d.strip() for d in description if d.strip()])
        
        # Extract all data attributes from elements
        data_attrs = {}
        elements = response.xpath('//*')  # Select all elements in the document
        for element in elements:
            for attr_name, attr_value in element.attrib.items():
                if attr_name.startswith('data-'):
                    data_attrs[attr_name] = attr_value
        data['data_attributes'] = data_attrs
        
        return data
