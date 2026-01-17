import gspread
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import json
import os

# Scopes required for Google Sheets API
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

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


def authenticate_google_sheets_service_account(credentials_file):
    """
    Authenticates with Google Sheets API using a service account.
    
    Args:
        credentials_file (str): Path to your Google service account JSON file
        
    Returns:
        gspread.Client: Authenticated gspread client
    """
    try:
        creds = Credentials.from_service_account_file(
            credentials_file, scopes=SCOPES)
        client = gspread.authorize(creds)
        print("Successfully authenticated with Google Sheets")
        return client
    except FileNotFoundError:
        print(f"Error: Credentials file not found at {credentials_file}")
        return None
    except Exception as e:
        print(f"Authentication error: {e}")
        return None


def get_urls_from_sheet(spreadsheet_url, sheet_name, column_name='URLs'):
    """
    Retrieves URLs from a Google Sheet.
    
    Args:
        spreadsheet_url (str): The Google Sheets URL or Spreadsheet ID
        sheet_name (str): Name of the worksheet tab
        column_name (str): Name of the column containing URLs (default: 'URLs')
        
    Returns:
        list: List of URLs found in the spreadsheet
    """
    try:
        # Try to find and use credentials file
        credentials_file = 'client_secret_489670801796-sel4dubflo3ojjo4bvl30a4f6do0708e.apps.googleusercontent.com.json'
        
        if not os.path.exists(credentials_file):
            print(f"Error: Credentials file not found at {credentials_file}")
            return []
        
        # Try OAuth2 authentication first
        client = authenticate_google_sheets_oauth(credentials_file)
        if not client:
            # Fall back to service account if OAuth fails
            client = authenticate_google_sheets_service_account(credentials_file)
        
        if not client:
            return []
        
        # Open the spreadsheet
        try:
            # Try opening by URL first
            spreadsheet = client.open_by_url(spreadsheet_url)
        except:
            # If that fails, try by ID
            spreadsheet = client.open_by_key(spreadsheet_url)
        
        # Get the specific worksheet
        worksheet = spreadsheet.worksheet(sheet_name)
        
        # Get all values from the sheet
        all_values = worksheet.get_all_values()
        
        if not all_values:
            print("Spreadsheet is empty")
            return []
        
        # Find the column index for the URLs column
        headers = all_values[0]
        try:
            column_index = headers.index(column_name)
        except ValueError:
            print(f"Column '{column_name}' not found. Available columns: {headers}")
            return []
        
        # Extract URLs from the column (skip header)
        urls = [row[column_index] for row in all_values[1:] if column_index < len(row) and row[column_index].strip()]
        
        print(f"Found {len(urls)} URLs in '{sheet_name}' sheet")
        return urls
        
    except Exception as e:
        print(f"Error reading from Google Sheets: {e}")
        return []


def save_urls_to_file(urls, output_file='urls.json'):
    """
    Saves URLs to a JSON file.
    
    Args:
        urls (list): List of URLs to save
        output_file (str): Output file path
    """
    try:
        with open(output_file, 'w') as f:
            json.dump({'urls': urls}, f, indent=2)
        print(f"Saved {len(urls)} URLs to {output_file}")
    except Exception as e:
        print(f"Error saving URLs: {e}")


if __name__ == "__main__":
    # Example usage
    print("=== Google Sheets URL Reader ===\n")
    
    # Your spreadsheet details
    SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1v7WQo630gSIHZSPitVmA3o4l1t9bE-cFRQRXyXdbz7E/edit?gid=1307754709#gid=1307754709"
    SHEET_NAME = "URL Sheet"  # The tab name in your Google Sheet
    COLUMN_NAME = "url"   # The column header for URLs
    
    spreadsheet_url = SPREADSHEET_URL
    sheet_name = SHEET_NAME
    column_name = COLUMN_NAME
    
    urls = get_urls_from_sheet(spreadsheet_url, sheet_name, column_name)
    if urls:
        print("\nURLs retrieved:")
        for url in urls:
            print(f"  - {url}")
        save_urls_to_file(urls)
    else:
        print("No URLs found or error occurred.")
