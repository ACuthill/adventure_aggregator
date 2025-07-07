# scrape_g_adventures_exploded.py

import requests
import time
from datetime import datetime, timezone

# --- NO PANDAS NEEDED ANYMORE ---
# import pandas as pd 

# --- NEW: Define the API URL ---
API_URL = "http://127.0.0.1:8000/adventures/"

def scrape_g_adventures_granular():
    """
    Scrapes G Adventures trips and "explodes" the data to create one row
    per departure date, providing granular, user-friendly data.
    """
    print("🚀 Starting G Adventures Granular Data Scraper...")

    api_url = "https://wylt2exvri-dsn.algolia.net/1/indexes/*/queries"
    headers = {
        'x-algolia-api-key': '15c4f91bdaf78324263405f8a9e72592',
        'x-algolia-application-id': 'WYLT2EXVRI',
        'Content-Type': 'application/json',
    }
    
    all_departures = []
    page_number = 0
    
    # Let's just scrape one page for testing to be quick
    # Set to 'while True' for a full scrape
    while page_number < 1: 
        print(f"\n--- Fetching page {page_number}... ---")
        payload = {
            "requests": [
                {
                    "indexName": "wwwtrips_en",
                    "params": f"clickAnalytics=true&facets=%5B%22destinations%22%2C%22duration%22%2C%22travelStyle%22%5D&filters=departureDates%3E0&hitsPerPage=50&page={page_number}&query="
                }
            ]
        }
        
        try:
            response = requests.post(api_url, headers=headers, json=payload, timeout=20)
            response.raise_for_status()
            data = response.json()
            hits = data.get('results', [{}])[0].get('hits', [])
            
            if not hits:
                print("    - No more trips found. Scrape complete.")
                break
            
            print(f"    - Found {len(hits)} trip dossiers on this page. Exploding departure dates...")
            
            for trip in hits:
                price_map_gbp = {p['startDate']: p['amount'] for p in trip.get('pricesGBP', []) if 'startDate' in p}
                default_price_gbp = trip.get('advertisedPriceGBP', {}).get('amount')
                departure_dates = trip.get('departureDates', [])
                if not departure_dates:
                    continue

                for date_timestamp in departure_dates:
                    specific_price = price_map_gbp.get(date_timestamp, default_price_gbp)
                    all_departures.append({
                        # These are the raw fields from the scraper
                        'trip_id': trip.get('tourDossierID'),
                        'trip_name': trip.get('name'),
                        'destinations': ", ".join(trip.get('destinations', [])),
                        'duration_days': trip.get('duration'),
                        'departure_date_unix': date_timestamp,
                        'price_gbp': specific_price,
                        'travel_style': trip.get('travelStyle'),
                        'url': 'https://www.gadventures.com/trips/' + trip.get('slug', ''),
                        'image_url': trip.get('images', {}).get('LARGE_SQUARE'),
                        'provider_name': 'G Adventures',
                        'objectID': f"{trip.get('objectID')}-{date_timestamp}"
                    })

            page_number += 1
            time.sleep(0.5)

        except Exception as e:
            print(f"    - An error occurred: {e}")
            break

    print(f"\n✅ API Scrape complete. Total granular departures extracted: {len(all_departures)}")
    return all_departures


def transform_for_api(scraped_adventure: dict) -> dict:
    """
    Transforms a single scraped adventure dictionary into the format
    expected by the AdventureCreate API schema.
    """
    # Convert Unix timestamp to a timezone-aware datetime object, then to an ISO 8601 string
    departure_dt = datetime.fromtimestamp(scraped_adventure.get('departure_date_unix', 0), tz=timezone.utc)
    
    # Map the scraped fields to the schema fields
    return {
        "unique_id": scraped_adventure.get('objectID'),
        "provider_name": "G Adventures",
        "trip_name": scraped_adventure.get('trip_name'),
        "url": scraped_adventure.get('url'),
        "image_url": scraped_adventure.get('image_url'),
        "price": scraped_adventure.get('price_gbp'),
        "currency": "GBP",  # We know this from the scraper's price field
        "departure_date": departure_dt.isoformat(),
        "duration": scraped_adventure.get('duration_days'),
        "location": scraped_adventure.get('destinations'),
        "activity_type": scraped_adventure.get('travel_style')
    }


def send_data_to_api(adventures: list):
    """Posts a list of scraped adventures to the aggregator API."""
    print(f"\n📨 Found {len(adventures)} adventures to send to the API.")
    
    success_count = 0
    for adventure in adventures:
        # Step 1: Transform the data to match the API schema
        api_payload = transform_for_api(adventure)

        # Basic validation: skip if we're missing a price or ID
        if not api_payload.get('price') or not api_payload.get('unique_id'):
            print(f"  - Skipping adventure due to missing price or unique_id: {api_payload.get('trip_name')}")
            continue

        try:
            # Step 2: Send the data to the API endpoint
            response = requests.post(API_URL, json=api_payload, timeout=10)

            if response.status_code == 200:
                print(f"  - ✅ Success: {api_payload['unique_id']}")
                success_count += 1
            else:
                print(f"  - ❌ Error upserting {api_payload['unique_id']}:")
                print(f"      Status Code: {response.status_code}")
                print(f"      Response: {response.text}")

        except requests.exceptions.RequestException as e:
            print(f"  - ❌ A critical network error occurred: {e}")
            break # Stop the process if the API is down
            
    print(f"\n🎉 Finished. Successfully sent {success_count}/{len(adventures)} adventures.")


if __name__ == "__main__":
    # --- THIS IS THE NEW WORKFLOW ---
    
    # 1. Scrape the data from the source as before
    scraped_departures = scrape_g_adventures_granular()
    
    # 2. If data was found, send it to our API
    if scraped_departures:
        send_data_to_api(scraped_departures)
    else:
        print("No adventures were scraped. Nothing to send.")