# scrape_explore_share.py (Version 3 - The Definitive API Scraper)

import requests
import pandas as pd
import time
from datetime import datetime

def scrape_explore_share():
    """
    Scrapes all trips from Explore Share using the correct Algolia index name
    and authentication-in-URL method, as discovered through debugging.
    """
    print("🚀 Starting Explore Share Definitive API Scraper...")

    # --- THE FIX IS HERE: Constructing the URL and Headers correctly ---
    # The API keys are part of the URL's query string, not the headers.
    base_url = "https://w22u5i7ng1-dsn.algolia.net/1/indexes/*/queries"
    api_key = "e23ea1a994c0c0190dee25712994b555"
    app_id = "W22U5I7NG1"
    
    # We add the auth keys as parameters to every request URL
    api_url_with_auth = f"{base_url}?x-algolia-agent=Algolia%20for%20JavaScript&x-algolia-api-key={api_key}&x-algolia-application-id={app_id}"

    # The headers are now simpler
    headers = {
        'Content-Type': 'application/json',
    }
    
    all_departures = []
    page_number = 0
    
    while True:
        print(f"\n--- Fetching page {page_number}... ---")

        # --- THE SECOND FIX: Using the correct indexName and params ---
        payload = {
            "requests": [
                {
                    "indexName": "es_searchable_posts_dato",
                    "params": f"filters=trip_status:Active&page={page_number}&hitsPerPage=50"
                }
            ]
        }
        
        try:
            # We now use the full URL that includes the authentication keys
            response = requests.post(api_url_with_auth, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            hits = data.get('results', [{}])[0].get('hits', [])
            
            if not hits:
                print("    - No more trips found. Scrape complete.")
                break
                
            print(f"    - Found {len(hits)} trip dossiers on this page. Exploding departure dates...")
            
            for trip in hits:
                open_groups = trip.get('open_groups', [])
                if not open_groups:
                    continue

                base_trip_info = {
                    'trip_id': trip.get('post_id'),
                    'trip_name': trip.get('post_title'),
                    'country': trip.get('taxonomies', {}).get('country', [None])[0],
                    'duration_days': trip.get('trip_extended_info_duration_in_days'),
                    'difficulty': trip.get('trip_extended_info_level'),
                    'main_activity': trip.get('taxonomies', {}).get('main_activity', [None])[0],
                    'guide_name': trip.get('post_author', {}).get('display_name'),
                    'url': trip.get('permalink'),
                    'image_url': trip.get('images', {}).get('medium', {}).get('url'),
                    'provider_name': 'Explore Share'
                }

                for group in open_groups:
                    departure_data = base_trip_info.copy()
                    departure_data['departure_date'] = group.get('departureDate')
                    departure_data['end_date'] = group.get('endDate')
                    departure_data['price'] = group.get('price')
                    departure_data['currency'] = trip.get('trip_currency')
                    departure_data['is_guaranteed'] = group.get('guaranteedTrip')
                    departure_data['objectID'] = f"{trip.get('objectID')}-{group.get('departureDate')}"
                    
                    all_departures.append(departure_data)

            page_number += 1
            time.sleep(0.5)

        except Exception as e:
            print(f"    - An error occurred: {e}")
            break

    print(f"\n✅ API Scrape complete. Total granular departures extracted: {len(all_departures)}")
    return all_departures

if __name__ == "__main__":
    scraped_departures = scrape_explore_share()
    
    if scraped_departures:
        df = pd.DataFrame(scraped_departures)
        df = df.drop_duplicates(subset=['objectID'])
        df['departure_date'] = pd.to_datetime(df['departure_date'], errors='coerce')
        df = df.sort_values(by=['trip_name', 'departure_date'])
        
        output_filename = 'explore_share_departures.csv'
        df.to_csv(output_filename, index=False)
        
        print(f"\nSUCCESS: Data saved to '{output_filename}'")
        print(f"Successfully scraped a total of {len(df)} unique departures.")
        print("\nFirst 10 rows of granular data:")
        print(df.head(10))