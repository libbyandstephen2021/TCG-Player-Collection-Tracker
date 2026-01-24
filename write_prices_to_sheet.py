import gspread
from google_auth_oauthlib.flow import InstalledAppFlow
import json
import os

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


def write_prices_to_sheet(spreadsheet_url, sheet_name, results, url_column='url', price_column='Price'):
    """
    Writes scraped prices back to a Google Sheet.
    
    Args:
        spreadsheet_url (str): The Google Sheets URL or Spreadsheet ID
        sheet_name (str): Name of the worksheet tab
        results (list): List of scrape results from scrape_results.json
        url_column (str): Name of the column containing URLs (default: 'url')
        price_column (str): Name of the column to write prices to (default: 'Price')
        
    Returns:
        dict: Summary of updates
    """
    try:
        credentials_file = 'client_secret_489670801796-sel4dubflo3ojjo4bvl30a4f6do0708e.apps.googleusercontent.com.json'
        
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
        
        # Get the specific worksheet
        worksheet = spreadsheet.worksheet(sheet_name)
        
        # Get all values from the sheet
        all_values = worksheet.get_all_values()
        
        if not all_values:
            return {'status': 'error', 'message': 'Spreadsheet is empty'}
        
        # Find column indices
        headers = all_values[0]
        try:
            url_column_index = headers.index(url_column)
        except ValueError:
            print(f"ERROR: Column '{url_column}' not found!")
            print(f"Available columns in sheet:")
            for i, header in enumerate(headers, 1):
                print(f"  Column {i}: '{header}'")
            return {'status': 'error', 'message': f"Column '{url_column}' not found. Available: {headers}"}
        
        try:
            price_column_index = headers.index(price_column)
        except ValueError:
            print(f"ERROR: Column '{price_column}' not found!")
            print(f"Available columns in sheet:")
            for i, header in enumerate(headers, 1):
                print(f"  Column {i}: '{header}'")
            return {'status': 'error', 'message': f"Column '{price_column}' not found. Available: {headers}"}
        
        # Create a mapping of URLs to prices from results
        price_map = {}
        for result in results:
            if result.get('status') == 'success':
                url = result.get('url')
                price = result.get('price_raw', result.get('price'))
                if url and price:
                    price_map[url] = price
        
        # Update the worksheet
        updated_count = 0
        not_found_count = 0
        
        for row_idx, row in enumerate(all_values[1:], 2):  # Start from row 2 (skip header)
            if url_column_index < len(row):
                url = row[url_column_index].strip()
                
                if url in price_map:
                    price = price_map[url]
                    worksheet.update_cell(row_idx, price_column_index + 1, price)
                    print(f"Updated row {row_idx}: {url} -> {price}")
                    updated_count += 1
                else:
                    not_found_count += 1
        
        summary = {
            'status': 'success',
            'updated_rows': updated_count,
            'not_found': not_found_count,
            'total_results': len(results),
            'message': f"Successfully updated {updated_count} rows"
        }
        
        return summary
        
    except Exception as e:
        print(f"Error writing to Google Sheets: {e}")
        return {'status': 'error', 'message': str(e)}


if __name__ == "__main__":
    print("=== TCG Price Writer to Google Sheets ===\n")
    
    # Load scrape results
    results = load_scrape_results('scrape_results.json')
    
    if not results:
        print("No scrape results found.")
    else:
        # Your spreadsheet details
        SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1v7WQo630gSIHZSPitVmA3o4l1t9bE-cFRQRXyXdbz7E/edit?gid=1307754709#gid=1307754709"
        SHEET_NAME = "URL Sheet"
        URL_COLUMN = "url"  # Column name with URLs
        PRICE_COLUMN = "Price"  # Column name to write prices to
        
        print(f"Writing {len(results)} results to Google Sheet...\n")
        
        summary = write_prices_to_sheet(SPREADSHEET_URL, SHEET_NAME, results, URL_COLUMN, PRICE_COLUMN)
        
        # Print summary
        print("\n" + "="*50)
        print("UPDATE SUMMARY")
        print("="*50)
        print(f"Status: {summary.get('status')}")
        print(f"Updated rows: {summary.get('updated_rows', 'N/A')}")
        print(f"Not found in results: {summary.get('not_found', 'N/A')}")
        print(f"Total results processed: {summary.get('total_results', 'N/A')}")
        print(f"Message: {summary.get('message')}")
