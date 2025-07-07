# scrape_mba_algolia_final.py (Version 4 - The Correct Payload Builder)

import requests
import pandas as pd
import time
from datetime import datetime
from urllib.parse import urlencode # Import the correct tool for the job

def scrape_all_trips_paginated():
    """
    Scrapes all trips using time-sliced pagination with a correctly constructed
    payload using Python's standard library.
    """
    print("🚀 Starting Time-Sliced Algolia API Scraper (Correct Payload Builder)...")

    api_url = "https://zovofxpu7b-dsn.algolia.net/1/indexes/*/queries"
    headers = {
        'x-algolia-api-key': '707ecfee47f2929dd64c183ec40c57e3',
        'x-algolia-application-id': 'ZOVOFXPU7B',
        'Content-Type': 'application/json',
    }
    
    all_trips = []
    last_start_date_timestamp = 0
    master_loop_count = 1

    while True:
        print(f"\n{'='*20} MASTER LOOP {master_loop_count} (Fetching trips after timestamp: {last_start_date_timestamp}) {'='*20}")
        
        page_number = 0
        batch_trips = []
        
        while True:
            # --- THE FINAL PAYLOAD FIX ---
            # Define the base parameters as a dictionary
            params_dict = {
                "clickAnalytics": "true",
                "facets": ["country", "departureMonth", "difficulty", "duration", "priceGBP"],
                "hitsPerPage": 48,
                "page": page_number,
                "tagFilters": ""
            }

            # Only add the numeric filter if we have a timestamp
            if last_start_date_timestamp > 0:
                # This is the correct JSON format for this filter
                params_dict["numericFilters"] = [f"startDate>{last_start_date_timestamp}"]

            # Use urlencode to correctly format the parameters into a string
            # doseq=True correctly handles list items like facets[]=...&facets[]=...
            params_string = urlencode(params_dict, doseq=True)

            payload = {
                "requests": [
                    {
                        "indexName": "prod_products__by_departure",
                        "params": params_string
                    }
                ]
            }
            
            print(f"    - Fetching internal page {page_number}...")
            try:
                response = requests.post(api_url, headers=headers, json=payload, timeout=20)
                response.raise_for_status()
                data = response.json()
                
                hits = data.get('results', [{}])[0].get('hits', [])
                
                if not hits:
                    print(f"    - No more trips found on internal page {page_number}. Ending this batch.")
                    break
                
                batch_trips.extend(hits)
                page_number += 1
                time.sleep(0.2)

            except Exception as e:
                print(f"    - Error on internal page {page_number}: {e}")
                import json
                print("    - Failing Payload:", json.dumps(payload))
                break

        if not batch_trips:
            print("\nMaster loop found no new trips. Scraping complete.")
            break
            
        print(f"    - This batch yielded {len(batch_trips)} trips.")
        all_trips.extend(batch_trips)
        
        batch_trips.sort(key=lambda x: x.get('startDate', 0))
        last_trip_in_batch = batch_trips[-1]
        last_start_date_timestamp = last_trip_in_batch.get('startDate', 0)
        
        if last_start_date_timestamp == 0:
            print("    - Could not determine next start date. Ending scrape.")
            break
            
        master_loop_count += 1
        
    print(f"\n✅ MASTER SCRAPE COMPLETE. Total unique trips extracted: {len(all_trips)}")
    return all_trips

if __name__ == "__main__":
    scraped_trips = scrape_all_trips_paginated()
    
    if scraped_trips:
        # Process the raw list of dictionaries into a clean DataFrame
        final_data = []
        for trip in scraped_trips:
            final_data.append({
                'trip_id': trip.get('productID'),
                'trip_name': trip.get('name'),
                'country': trip.get('country', [None])[0],
                'duration_days': trip.get('duration'),
                'price_gbp': trip.get('priceGBP'),
                'difficulty': trip.get('difficulty', [None])[0],
                'review_score': trip.get('reviewScore'),
                'review_count': trip.get('reviewCount'),
                'availability_spaces': trip.get('spacesLeft'),
                'start_date_unix': trip.get('startDate'),
                'subtitle': trip.get('subtitle'),
                'activity': trip.get('activity', []),
                'url': 'https://www.muchbetteradventures.com' + trip.get('url', ''),
                'image_url': trip.get('image', {}).get('url'),
                'objectID': trip.get('objectID')
            })

        df = pd.DataFrame(final_data)
        df = df.drop_duplicates(subset=['objectID'])
        df['start_date'] = pd.to_datetime(df['start_date_unix'], unit='s', errors='coerce').dt.date
        
        output_filename = 'mba_api_all_3000_trips.csv'
        df.to_csv(output_filename, index=False)
        
        print(f"\nSUCCESS: Data saved to '{output_filename}'")
        print(f"Successfully scraped a total of {len(df)} unique trips.")