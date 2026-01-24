"""
TCG Player Collection Tracker - Full Pipeline
Runs the complete flow: Read URLs -> Scrape Prices -> Write Results
"""

import json
import os
import sys
from datetime import datetime

# Import functions from other scripts
from google_sheets_reader import get_urls_from_sheet, save_urls_to_file
from TCG-URL-Scraper-Draft import scrape_multiple_products, load_urls_from_file
from write_prices_to_sheet import write_prices_to_sheet, load_scrape_results


def run_full_pipeline():
    """
    Runs the complete TCG scraping pipeline:
    1. Reads URLs from Google Sheet
    2. Scrapes prices from each URL
    3. Writes results back to Google Sheet
    """
    
    print("="*60)
    print("TCG PLAYER COLLECTION TRACKER - FULL PIPELINE")
    print("="*60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Configuration
    SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1v7WQo630gSIHZSPitVmA3o4l1t9bE-cFRQRXyXdbz7E/edit?gid=1307754709#gid=1307754709"
    SHEET_NAME = "URL Sheet"
    URL_COLUMN = "url"
    PRICE_COLUMN = "price"
    
    # Step 1: Read URLs from Google Sheet
    print("\n" + "="*60)
    print("STEP 1: READING URLS FROM GOOGLE SHEET")
    print("="*60)
    
    try:
        urls = get_urls_from_sheet(SPREADSHEET_URL, SHEET_NAME, URL_COLUMN)
        
        if not urls:
            print("ERROR: No URLs found in Google Sheet")
            return False
        
        print(f"✓ Successfully retrieved {len(urls)} URLs")
        
        # Save URLs to file for reference
        save_urls_to_file(urls, 'urls.json')
        
    except Exception as e:
        print(f"✗ Error reading from Google Sheet: {e}")
        return False
    
    # Step 2: Scrape prices
    print("\n" + "="*60)
    print("STEP 2: SCRAPING PRICES FROM URLS")
    print("="*60)
    
    try:
        print(f"Starting to scrape {len(urls)} URLs...\n")
        results = scrape_multiple_products(urls)
        
        # Count successes and failures
        successful = [r for r in results if r['status'] == 'success']
        failed = [r for r in results if r['status'] == 'error']
        
        print(f"\n✓ Scraping complete!")
        print(f"  - Successful: {len(successful)}")
        print(f"  - Failed: {len(failed)}")
        
        # Save results to file
        with open('scrape_results.json', 'w') as f:
            json.dump(results, f, indent=2)
        print(f"  - Saved to: scrape_results.json")
        
    except Exception as e:
        print(f"✗ Error during scraping: {e}")
        return False
    
    # Step 3: Write results back to Google Sheet
    print("\n" + "="*60)
    print("STEP 3: WRITING RESULTS TO GOOGLE SHEET")
    print("="*60)
    
    try:
        summary = write_prices_to_sheet(SPREADSHEET_URL, SHEET_NAME, results, URL_COLUMN, PRICE_COLUMN)
        
        if summary.get('status') == 'success':
            print(f"✓ Successfully updated Google Sheet!")
            print(f"  - Updated rows: {summary.get('updated_rows', 0)}")
            print(f"  - Not found: {summary.get('not_found', 0)}")
            print(f"  - Total processed: {summary.get('total_results', 0)}")
        else:
            print(f"✗ Error writing to Google Sheet: {summary.get('message')}")
            return False
            
    except Exception as e:
        print(f"✗ Error writing to Google Sheet: {e}")
        return False
    
    # Summary
    print("\n" + "="*60)
    print("PIPELINE COMPLETE")
    print("="*60)
    print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"\nSummary:")
    print(f"  - URLs read from sheet: {len(urls)}")
    print(f"  - Prices scraped: {len(successful)}")
    print(f"  - Prices written to sheet: {summary.get('updated_rows', 0)}")
    print(f"  - Errors: {len(failed)}")
    
    return True


if __name__ == "__main__":
    try:
        success = run_full_pipeline()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n✗ Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        sys.exit(1)
