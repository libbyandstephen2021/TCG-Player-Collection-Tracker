from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import json
import time

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
    for url in urls:
        print(f"\nScraping: {url}")
        data = scrape_tcg_price(url)
        results.append(data)
        time.sleep(1)  # Be respectful to the server - wait 1 second between requests
    
    return results


if __name__ == "__main__":
    # Example usage
    test_url = "https://www.tcgplayer.com/product/504467?Language=English"
    
    print("=== TCG Player Price Scraper ===\n")
    result = scrape_tcg_price(test_url)
    
    # Print results as JSON
    print("\nResult:")
    print(json.dumps(result, indent=2))
    
    # Example: scraping multiple products
    # multiple_urls = [
    #     "https://www.tcgplayer.com/product/504467?Language=English",
    #     # Add more URLs here
    # ]
    # results = scrape_multiple_products(multiple_urls)
    # print(json.dumps(results, indent=2))
