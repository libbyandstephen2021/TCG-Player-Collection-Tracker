import gspread
from google_auth_oauthlib.flow import InstalledAppFlow
import json
import os
from datetime import datetime
from urllib.parse import urlparse, parse_qs

# Scopes required for Google Sheets API
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

def authenticate_google_sheets_oauth(credentials_file):
    """
    Authenticates with Google Sheets API using OAuth2 (installed app).
    
    Args:
        credentials_file (str): Path to your OAuth2 credentials JSON file
        
    Returns:
        gspread.Client: Authenticated gspread client
    """
    try:
        flow = InstalledAppFlow.from_client_secrets_file(
            credentials_file, SCOPES)
        creds = flow.run_local_server(port=0)
        client = gspread.authorize(creds)
        print("Successfully authenticated with Google Sheets")
        return client
    except FileNotFoundError:
        print(f"Error: Credentials file not found at {credentials_file}")
        return None
    except Exception as e:
        print(f"Authentication error: {e}")
        return None


def load_scrape_results(filename='scrape_results.json'):
    """
    Loads scraping results from a JSON file.
    
    Args:
        filename (str): Path to the scrape results JSON file
        
    Returns:
        list: List of scrape result dictionaries
    """
    try:
        if not os.path.exists(filename):
            print(f"Error: {filename} not found.")
            return []
        
        with open(filename, 'r') as f:
            results = json.load(f)
        
        print(f"Loaded {len(results)} scrape results from {filename}")
        return results
    except Exception as e:
        print(f"Error loading scrape results: {e}")
        return []


def get_card_info_from_sheet(spreadsheet_url, sheet_name, credentials_file):
    """
    Retrieves card names and numbers from the URL sheet.
    
    Args:
        spreadsheet_url (str): The Google Sheets URL or Spreadsheet ID
        sheet_name (str): Name of the worksheet tab with URLs
        credentials_file (str): Path to credentials file
        
    Returns:
        dict: Mapping of URL to {card_name, card_number}
    """
    try:
        flow = InstalledAppFlow.from_client_secrets_file(
            credentials_file, SCOPES)
        creds = flow.run_local_server(port=0)
        client = gspread.authorize(creds)
        
        # Open the spreadsheet
        try:
            spreadsheet = client.open_by_url(spreadsheet_url)
        except:
            spreadsheet = client.open_by_key(spreadsheet_url)
        
        # Get the specific worksheet
        worksheet = spreadsheet.worksheet(sheet_name)
        
        # Get all values from the sheet
        all_values = worksheet.get_all_values()
        
        if not all_values:
            return {}
        
        # Find column indices
        headers = all_values[0]
        print(f"Headers found: {headers}")
        
        try:
            url_index = headers.index('url')
        except ValueError:
            print("Warning: 'url' column not found in URL sheet")
            return {}
        
        try:
            card_name_index = headers.index('Card Name')
            print(f"'Card Name' column found at index {card_name_index}")
        except ValueError:
            print("Warning: 'Card Name' column not found in URL sheet")
            print(f"Available columns: {headers}")
            card_name_index = None
        
        try:
            card_number_index = headers.index('Card Number')
            print(f"'Card Number' column found at index {card_number_index}")
        except ValueError:
            print("Warning: 'Card Number' column not found in URL sheet")
            print(f"Available columns: {headers}")
            card_number_index = None
        
        # Create mapping of URL to card info
        card_map = {}
        for row in all_values[1:]:
            if url_index < len(row) and row[url_index].strip():
                url = row[url_index].strip()
                card_name = row[card_name_index].strip() if card_name_index is not None and card_name_index < len(row) else "N/A"
                card_number = row[card_number_index].strip() if card_number_index is not None and card_number_index < len(row) else "N/A"
                
                card_map[url] = {
                    'card_name': card_name,
                    'card_number': card_number
                }
        
        print(f"Retrieved card info for {len(card_map)} URLs")
        if card_map:
            print(f"Sample: {list(card_map.items())[0]}")
        return card_map
        
    except Exception as e:
        print(f"Warning: Could not retrieve card info from sheet: {e}")
        return {}


def extract_condition_from_url(url):
    """
    Extracts the 'Condition' parameter from a URL query string.
    
    Args:
        url (str): The URL to parse
        
    Returns:
        str: The condition value (e.g., 'Near Mint') or 'N/A' if not found
    """
    try:
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        
        # parse_qs returns lists for each parameter, so we get the first value
        if 'Condition' in query_params:
            condition = query_params['Condition'][0]
            return condition
        else:
            return 'N/A'
    except Exception as e:
        print(f"Warning: Could not parse condition from URL: {e}")
        return 'N/A'


def append_prices_to_sheet(spreadsheet_url, source_sheet_name, archive_sheet_name, results, credentials_file):
    """
    Appends scraped prices as new rows to a Google Sheet with card info and today's date.
    Each run creates a new row per URL, including Card Name and Card Number from source sheet.
    
    Args:
        spreadsheet_url (str): The Google Sheets URL or Spreadsheet ID
        source_sheet_name (str): Name of the worksheet tab with URLs (source)
        archive_sheet_name (str): Name of the worksheet tab to append prices to (archive)
        results (list): List of scrape results from scrape_results.json
        credentials_file (str): Path to credentials file
        
    Returns:
        dict: Summary of updates
    """
    try:
        # First, get card info from the source sheet
        print("Retrieving card information from URL sheet...")
        card_map = get_card_info_from_sheet(spreadsheet_url, source_sheet_name, credentials_file)
        
        if not os.path.exists(credentials_file):
            print(f"Error: Credentials file not found at {credentials_file}")
            return {'status': 'error', 'message': 'Credentials file not found'}
        
        client = authenticate_google_sheets_oauth(credentials_file)
        if not client:
            return {'status': 'error', 'message': 'Authentication failed'}
        
        # Open the spreadsheet
        try:
            spreadsheet = client.open_by_url(spreadsheet_url)
        except:
            spreadsheet = client.open_by_key(spreadsheet_url)
        
        # Get the archive worksheet
        worksheet = spreadsheet.worksheet(archive_sheet_name)
        
        # Get today's date
        today = datetime.now().strftime('%Y-%m-%d')
        
        # Prepare rows to append
        rows_to_append = []
        appended_count = 0
        
        for result in results:
            if result.get('status') == 'success':
                url = result.get('url')
                price = result.get('price_raw', result.get('price'))
                
                if url and price:
                    # Get card info from the mapping
                    card_info = card_map.get(url, {'card_name': 'N/A', 'card_number': 'N/A'})
                    card_name = card_info.get('card_name', 'N/A')
                    card_number = card_info.get('card_number', 'N/A')
                    
                    # Extract condition from URL
                    condition = extract_condition_from_url(url)
                    
                    # Create a new row: [Card Name, Card Number, Date, Price, Condition]
                    row = [card_name, card_number, today, price, condition]
                    rows_to_append.append(row)
                    appended_count += 1
        
        if rows_to_append:
            # Append all rows at once
            worksheet.append_rows(rows_to_append)
            print(f"Appended {appended_count} rows to Google Sheet")
            
            summary = {
                'status': 'success',
                'appended_rows': appended_count,
                'date': today,
                'message': f"Successfully appended {appended_count} rows with date {today}"
            }
        else:
            summary = {
                'status': 'success',
                'appended_rows': 0,
                'date': today,
                'message': "No successful results to append"
            }
        
        return summary
        
    except Exception as e:
        print(f"Error appending to Google Sheets: {e}")
        return {'status': 'error', 'message': str(e)}


if __name__ == "__main__":
    print("=== TCG Price Appender to Google Sheets ===\n")
    
    # Your spreadsheet details
    SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1v7WQo630gSIHZSPitVmA3o4l1t9bE-cFRQRXyXdbz7E/edit?gid=1307754709#gid=1307754709"
    SOURCE_SHEET_NAME = "URL Sheet"  # Sheet with URLs and card info
    ARCHIVE_SHEET_NAME = "Final Script Output"  # Sheet where you want to archive prices
    CREDENTIALS_FILE = 'client_secret_489670801796-sel4dubflo3ojjo4bvl30a4f6do0708e.apps.googleusercontent.com.json'
    
    # Load scrape results
    results = load_scrape_results('scrape_results.json')
    
    if not results:
        print("No scrape results found.")
    else:
        print(f"Appending {len(results)} results to Google Sheet...\n")
        
        summary = append_prices_to_sheet(SPREADSHEET_URL, SOURCE_SHEET_NAME, ARCHIVE_SHEET_NAME, results, CREDENTIALS_FILE)
        
        # Print summary
        print("\n" + "="*50)
        print("APPEND SUMMARY")
        print("="*50)
        print(f"Status: {summary.get('status')}")
        print(f"Appended rows: {summary.get('appended_rows', 'N/A')}")
        print(f"Date: {summary.get('date', 'N/A')}")
        print(f"Message: {summary.get('message')}")
