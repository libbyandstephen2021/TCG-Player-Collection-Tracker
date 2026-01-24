from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import json
import time
import random
import os

def scrape_tcg_price(url):
    """
    Scrapes the market price from a TCGPlayer product page using Selenium.
    
    Args:
        url (str): The TCGPlayer product URL
        
    Returns:
        dict: A dictionary containing the scraped data
    """
    driver = None
    try:
        # Create a Chrome WebDriver with minimal options
        options = webdriver.ChromeOptions()
        options.add_argument("--start-maximized")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        
        print(f"Fetching: {url}")
        driver.get(url)
        
        # Wait for the price element to load (up to 10 seconds)
        try:
            # First, wait for the "Market Price" header to be present
            market_price_header = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//span[contains(text(), 'Market Price')]"))
            )
            print("Market Price section found")
            
            # Find the parent container of the Market Price section
            market_price_container = market_price_header.find_element(By.XPATH, "ancestor::div[contains(@class, 'price-points__upper')]")
            
            # Find the price value within this specific container
            price_element = market_price_container.find_element(By.CLASS_NAME, "price-points__upper__price")
            price_text = price_element.text.strip()
            print(f"Market Price found: {price_text}")
            
            # Extract the numeric value (remove $ and convert to float)
            price_value = price_text.replace('$', '')
            
            return {
                'status': 'success',
                'url': url,
                'price': price_value,
                'price_raw': price_text,
                'section': 'Market Price'
            }
        except Exception as e:
            print(f"Market Price element not found: {e}")
            # Fallback: Parse the page source with BeautifulSoup for alternative methods
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # Look for the Market Price section specifically
            market_price_header = soup.find('span', class_='price-points__upper__header__title', string=lambda s: s and 'Market Price' in s)
            if market_price_header:
                # Find the parent price-points__upper container
                price_container = market_price_header.find_parent('div', class_='price-points__upper')
                if price_container:
                    price_span = price_container.find('span', class_='price-points__upper__price')
                    if price_span:
                        price_text = price_span.get_text(strip=True)
                        print(f"Market Price found (alternative): {price_text}")
                        price_value = price_text.replace('$', '')
                        return {
                            'status': 'success',
                            'url': url,
                            'price': price_value,
                            'price_raw': price_text,
                            'section': 'Market Price'
                        }
            
            return {
                'status': 'error',
                'url': url,
                'message': 'Market Price element not found after waiting'
            }
            
    except Exception as e:
        print(f"An error occurred: {e}")
        return {
            'status': 'error',
            'url': url,
            'message': str(e)
        }
    finally:
        if driver:
            driver.quit()


def scrape_multiple_products(urls):
    """
    Scrapes multiple TCGPlayer product URLs.
    
    Args:
        urls (list): List of TCGPlayer product URLs
        
    Returns:
        list: List of dictionaries containing scraped data
    """
    results = []
    for i, url in enumerate(urls, 1):
        print(f"\n[{i}/{len(urls)}] Scraping: {url}")
        data = scrape_tcg_price(url)
        results.append(data)
        if i < len(urls):  # Don't sleep after the last URL
            delay = random.uniform(11, 15)
            print(f"Waiting {delay:.1f} seconds before next request...")
            time.sleep(delay)  # Be respectful to the server - random delay between 11-15 seconds
    
    return results


def load_urls_from_file(filename='urls.json'):
    """
    Loads URLs from a JSON file.
    
    Args:
        filename (str): Path to the JSON file
        
    Returns:
        list: List of URLs
    """
    try:
        if not os.path.exists(filename):
            print(f"Error: {filename} not found.")
            return []
        
        with open(filename, 'r') as f:
            data = json.load(f)
            # Handle different possible JSON structures
            if isinstance(data, dict) and 'urls' in data:
                urls = data['urls']
            elif isinstance(data, list):
                urls = data
            else:
                print("Invalid JSON structure. Expected {'urls': [...]} or [...]")
                return []
        
        print(f"Loaded {len(urls)} URLs from {filename}")
        return urls
    except Exception as e:
        print(f"Error loading URLs from {filename}: {e}")
        return []


def save_results_to_file(results, output_file='scrape_results.json'):
    """
    Saves scraping results to a JSON file.
    
    Args:
        results (list): List of scraping results
        output_file (str): Output file path
    """
    try:
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"\nSaved results for {len(results)} URLs to {output_file}")
    except Exception as e:
        print(f"Error saving results: {e}")


if __name__ == "__main__":
    print("=== TCG Player Price Scraper ===\n")
    
    # Load URLs from the JSON file
    urls = load_urls_from_file('urls.json')
    
    if not urls:
        print("No URLs found. Make sure urls.json exists and contains URLs.")
    else:
        print(f"\nStarting to scrape {len(urls)} URLs...\n")
        
        # Scrape all URLs
        results = scrape_multiple_products(urls)
        
        # Print summary
        print("\n" + "="*50)
        print("SCRAPING SUMMARY")
        print("="*50)
        
        successful = [r for r in results if r['status'] == 'success']
        failed = [r for r in results if r['status'] == 'error']
        
        print(f"Total URLs: {len(results)}")
        print(f"Successful: {len(successful)}")
        print(f"Failed: {len(failed)}")
        
        if successful:
            print("\n--- Successful Results ---")
            for result in successful:
                print(f"URL: {result['url']}")
                print(f"Price: {result['price_raw']}")
                print()
        
        if failed:
            print("\n--- Failed Results ---")
            for result in failed:
                print(f"URL: {result['url']}")
                print(f"Error: {result['message']}")
                print()
        
        # Save results to file
        save_results_to_file(results)
