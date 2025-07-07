# scrape_mapotapo_playwright.py (Version 8 - The Un-crashable Parser)

import asyncio
from playwright.async_api import async_playwright
import pandas as pd
from datetime import datetime

async def scrape_mapo_tapo_accordion():
    """
    FINAL WORKING VERSION: This script uses un-crashable, hyper-specific
    locators to handle all variations in the trip card HTML (e.g., sale prices).
    """
    print("🚀 Starting Mapo Tapo Final Scraper (Un-crashable Parser)...")

    target_url = "https://www.mapotapo.com/search?view=calendar"
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True) # Set to False to watch
        context = await browser.new_context(permissions=["geolocation"])
        page = await context.new_page()
        page.set_default_timeout(30000)
        await page.set_viewport_size({"width": 1920, "height": 1080})

        print(f"    - Navigating to: {target_url}")
        await page.goto(target_url, wait_until="load")

        print("    - Hunting for cookie banners...")
        try:
            await page.get_by_role("button", name="Accept all").click(timeout=5000)
        except Exception: pass
        try:
            await page.locator("button.iubenda-cs-accept-btn").click(timeout=5000)
            print("    - SUCCESS: Handled cookie banners.")
        except Exception: pass
        await page.wait_for_timeout(1000)

        month_buttons = await page.locator('button[id^="headlessui-disclosure-button-"]').all()
        print(f"    - Found {len(month_buttons)} months to process.")
        
        all_trips_data = []

        for i, button in enumerate(month_buttons):
            month_year_text = "N/A"
            try:
                month_year_text = await button.locator('p.font-bold').inner_text()
                print(f"\n--- Expanding: {month_year_text} (Button {i+1}/{len(month_buttons)}) ---")

                if await button.get_attribute("aria-expanded") == "false":
                    await button.click()
                    await page.wait_for_timeout(1500)

                panel_locator = button.locator("xpath=following-sibling::div[1]")
                trip_rows = await panel_locator.locator("div > div.group").all()
                print(f"    - Found {len(trip_rows)} unique trip rows in this panel.")

                for row in trip_rows:
                    # --- UN-CRASHABLE, HYPER-SPECIFIC PARSING ---
                    trip_data = {}
                    try:
                        # Trip Name
                        trip_data['trip_name'] = await row.locator('p.font-bold.text-lg.uppercase').inner_text()
                        
                        # URL
                        trip_data['url'] = 'https://www.mapotapo.com' + await row.locator('a').first.get_attribute('href')
                        
                        # Date
                        date_container = row.locator('div.flex.flex-col').first
                        month_abbr = await date_container.locator('p').first.inner_text()
                        day = await date_container.locator('p').nth(1).inner_text()
                        trip_data['departure_date'] = f"{day} {month_abbr} {month_year_text.split(' ')[1]}"
                        
                        # Location
                        location_element = row.locator('div.bg-secondary p')
                        trip_data['location'] = await location_element.inner_text() if await location_element.count() > 0 else "N/A"

                        # Price (handles sales by finding the one NOT struck-through)
                        price_element = row.locator("b.font-black:not([class*='line-through'])")
                        trip_data['price'] = await price_element.inner_text() if await price_element.count() > 0 else "N/A"

                        # Status
                        status_element = row.locator("div.rounded-full:has(p:has-text('Sold Out'))")
                        trip_data['status'] = "Sold Out" if await status_element.count() > 0 else "Available"
                        
                        # Date Range / Duration
                        date_range_element = row.locator('p:has-text("|")') # The pipe is a unique separator
                        trip_data['date_range_text'] = await date_range_element.inner_text() if await date_range_element.count() > 0 else "N/A"
                        
                        # Add static data
                        trip_data['provider_name'] = 'Mapo Tapo'
                        trip_data['scraped_at'] = datetime.now().isoformat()
                        
                        all_trips_data.append(trip_data)

                    except Exception as e_card:
                        print(f"        - ERROR parsing a single card detail: {e_card}")
                        continue
                
                if await button.get_attribute("aria-expanded") == "true":
                    await button.click()

            except Exception as e_group:
                print(f"      - ERROR processing month group '{month_year_text}': {e_group}")
                continue

        await browser.close()

    print(f"\n✅ Scraping complete. Total trips extracted: {len(all_trips_data)}")
    return all_trips_data


if __name__ == "__main__":
    scraped_trips = asyncio.run(scrape_mapo_tapo_accordion())
    
    if scraped_trips:
        df = pd.DataFrame(scraped_trips)
        output_filename = 'mapotapo_calendar_trips_final.csv'
        df.to_csv(output_filename, index=False)
        
        print(f"\nSUCCESS: Data saved to '{output_filename}'")
        print(f"Successfully scraped a total of {len(df)} trips.")
        print("\nFirst 10 rows of data:")
        print(df.head(10))