#!/usr/bin/env python3
import os
import json
import logging
import datetime
import time
from typing import Dict, Any, Optional

import trino
import pandas as pd
from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Google API scopes
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

def load_config() -> Dict[str, Any]:
    """Load configuration from environment variables."""
    load_dotenv()
    
    required_vars = [
        'TRINO_HOST', 
        'TRINO_PORT', 
        'TRINO_USER', 
        'TRINO_CATALOG', 
        'TRINO_SCHEMA',
        'GOOGLE_CLIENT_SECRET_FILE',
        'TOKEN_PATH',
        'DRIVE_FOLDER_ID',
    ]
    
    # Check if all required environment variables are set
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    return {
        'trino': {
            'host': os.getenv('TRINO_HOST'),
            'port': int(os.getenv('TRINO_PORT')),
            'user': os.getenv('TRINO_USER'),
            'password': os.getenv('TRINO_PASSWORD'),
            'catalog': os.getenv('TRINO_CATALOG'),
            'schema': os.getenv('TRINO_SCHEMA'),
        },
        'google': {
            'client_secret_file': os.getenv('GOOGLE_CLIENT_SECRET_FILE'),
            'token_path': os.getenv('TOKEN_PATH'),
            'drive_folder_id': os.getenv('DRIVE_FOLDER_ID'),
        }
    }

def read_sql_from_file(file_path: str) -> str:
    """Read SQL query from a file."""
    logger.info(f"Reading SQL query from {file_path}")
    try:
        with open(file_path, 'r') as file:
            return file.read()
    except Exception as e:
        raise RuntimeError(f"Failed to read SQL file {file_path}: {e}")

def get_google_credentials(client_secret_file: str, token_path: str) -> Credentials:
    """Get or refresh Google API credentials."""
    credentials = None
    
    # Check if token file exists
    if os.path.exists(token_path):
        logger.info("Loading existing token")
        try:
            with open(token_path, 'r') as token:
                credentials = Credentials.from_authorized_user_info(
                    json.loads(token.read()), SCOPES
                )
        except Exception as e:
            logger.warning(f"Error loading existing token: {e}")
    
    # If credentials don't exist or are invalid, get new ones
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            logger.info("Refreshing expired token")
            try:
                credentials.refresh(Request())
            except Exception as e:
                logger.warning(f"Error refreshing token: {e}")
                credentials = None
        
        # If still no valid credentials, initiate OAuth 2.0 flow
        if not credentials or not credentials.valid:
            logger.info("Initiating OAuth 2.0 flow")
            try:
                flow = InstalledAppFlow.from_client_secrets_file(
                    client_secret_file, SCOPES
                )
                credentials = flow.run_local_server(port=0)
            except Exception as e:
                raise RuntimeError(f"Failed to authenticate with Google: {e}")
            
            # Save credentials for future use
            with open(token_path, 'w') as token:
                token.write(credentials.to_json())
            logger.info(f"Token saved to {token_path}")
    
    return credentials

def execute_trino_query(config: Dict[str, Any], sql_query: str) -> pd.DataFrame:
    """Connect to Trino database and execute query, returning a pandas DataFrame."""
    logger.info("Connecting to Trino database")
    try:
        conn = trino.dbapi.connect(
            host=config['trino']['host'],
            port=config['trino']['port'],
            user=config['trino']['user'],
            catalog=config['trino']['catalog'],
            schema=config['trino']['schema'],
            http_scheme='https',
            auth=trino.auth.BasicAuthentication(config['trino']['user'], config['trino']['password'])
        )
        
        logger.info("Executing SQL query")
        # Use pandas to directly read from the SQL connection
        df = pd.read_sql(sql_query, conn)
        
        logger.info(f"Query executed successfully. Fetched {len(df)} rows")
        
        # Close connection
        conn.close()
        
        return df
    
    except Exception as e:
        raise RuntimeError(f"Failed to execute Trino query: {e}")

def prepare_dataframe_for_sheets(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare DataFrame for Google Sheets by converting all values to JSON-serializable types.
    
    This function:
    1. Creates a deep copy of the DataFrame
    2. Converts date/datetime columns to strings
    3. Converts any non-serializable values to strings
    4. Replaces NaN/None values with None (which is JSON serializable)
    
    Returns:
        Modified DataFrame with all values JSON-serializable
    """
    # Make a deep copy to avoid modifying the original
    df_copy = df.copy(deep=True)
    
    # Convert date/datetime columns to strings
    date_cols = df_copy.select_dtypes(include=['datetime', 'datetime64', 'datetime64[ns]']).columns
    for col in date_cols:
        df_copy[col] = df_copy[col].astype(str)
    
    # Replace NaN/None with None (which is JSON serializable)
    df_copy = df_copy.replace({pd.NA: None, pd.NaT: None})
    df_copy = df_copy.where(pd.notnull(df_copy), None)
    
    # Verify all columns are serializable and convert if necessary
    for col in df_copy.columns:
        try:
            # Test JSON serialization on this column
            json.dumps(df_copy[col].tolist())
        except (TypeError, OverflowError):
            # If not serializable, convert to strings
            df_copy[col] = df_copy[col].astype(str)
    
    # Test the entire DataFrame can be serialized
    try:
        json.dumps(df_copy.values.tolist())
    except (TypeError, OverflowError) as e:
        logger.warning(f"DataFrame still contains non-serializable values: {e}")
        # Last resort: convert the entire DataFrame to strings
        for col in df_copy.columns:
            df_copy[col] = df_copy[col].astype(str)
    
    return df_copy

def create_google_sheet(credentials: Credentials, title: str) -> str:
    """Create a new Google Sheet and return its ID."""
    logger.info(f"Creating new Google Sheet: {title}")
    
    # Standard retry parameters
    MAX_RETRIES = 5
    INITIAL_RETRY_DELAY = 1
    
    for retry in range(MAX_RETRIES):
        try:
            # Build the service
            sheets_service = build('sheets', 'v4', credentials=credentials)
            
            # Create spreadsheet
            spreadsheet = {
                'properties': {
                    'title': title
                }
            }
            
            response = sheets_service.spreadsheets().create(
                body=spreadsheet, 
                fields='spreadsheetId'
            ).execute()
            
            spreadsheet_id = response.get('spreadsheetId')
            logger.info(f"Created spreadsheet with ID: {spreadsheet_id}")
            
            return spreadsheet_id
            
        except HttpError as e:
            if retry >= MAX_RETRIES - 1 or e.resp.status not in [429, 500, 502, 503, 504]:
                # If we've exhausted retries or the error is not retryable, raise it
                raise RuntimeError(f"Failed to create Google Sheet: {e}")
            
            # Calculate backoff time
            wait_time = INITIAL_RETRY_DELAY * (2 ** retry)
            logger.warning(f"Google API error: {e}. Retrying in {wait_time} seconds (attempt {retry+1}/{MAX_RETRIES})")
            time.sleep(wait_time)
        
        except Exception as e:
            raise RuntimeError(f"Failed to create Google Sheet: {e}")
    
    # This should never be reached due to the raise in the except block
    raise RuntimeError("Failed to create Google Sheet after retries")

def write_dataframe_to_sheet(
    credentials: Credentials,
    spreadsheet_id: str,
    df: pd.DataFrame
) -> None:
    """Write pandas DataFrame to the Google Sheet."""
    logger.info("Writing data to Google Sheet")
    
    # Standard retry parameters
    MAX_RETRIES = 5
    INITIAL_RETRY_DELAY = 1
    
    # Convert DataFrame to serializable format
    logger.info("Preparing data for serialization")
    df_prepared = prepare_dataframe_for_sheets(df)
    
    # Convert to values list
    values = [df_prepared.columns.tolist()] + df_prepared.values.tolist()
    
    # Use batching for large datasets (Google Sheets has limits)
    BATCH_SIZE = 5000
    
    # Split into batches if necessary
    if len(values) > BATCH_SIZE:
        batches = [values[i:i + BATCH_SIZE] for i in range(0, len(values), BATCH_SIZE)]
        logger.info(f"Data too large, splitting into {len(batches)} batches")
        
        for retry in range(MAX_RETRIES):
            try:
                # Build the service for each attempt to avoid stale connections
                sheets_service = build('sheets', 'v4', credentials=credentials)
                
                # Process batches
                for i, batch in enumerate(batches):
                    start_range = f'Sheet1!A{1 if i == 0 else (i * BATCH_SIZE) + 1}'
                    
                    # Use update for first batch (with headers), append for the rest
                    if i == 0:
                        sheets_service.spreadsheets().values().update(
                            spreadsheetId=spreadsheet_id,
                            range=start_range,
                            valueInputOption='RAW',
                            body={'values': batch}
                        ).execute()
                    else:
                        sheets_service.spreadsheets().values().append(
                            spreadsheetId=spreadsheet_id,
                            range=start_range,
                            valueInputOption='RAW',
                            insertDataOption='INSERT_ROWS',
                            body={'values': batch}
                        ).execute()
                    
                    logger.info(f"Batch {i+1}/{len(batches)} written")
                
                logger.info(f"Successfully wrote {len(df)} rows to Google Sheet")
                return
                
            except HttpError as e:
                if retry >= MAX_RETRIES - 1 or e.resp.status not in [429, 500, 502, 503, 504]:
                    raise RuntimeError(f"Failed to write data to Google Sheet: {e}")
                
                wait_time = INITIAL_RETRY_DELAY * (2 ** retry)
                logger.warning(f"Google API error: {e}. Retrying in {wait_time} seconds (attempt {retry+1}/{MAX_RETRIES})")
                time.sleep(wait_time)
            
            except Exception as e:
                raise RuntimeError(f"Failed to write data to Google Sheet: {e}")
    else:
        # For smaller datasets, write in a single operation
        for retry in range(MAX_RETRIES):
            try:
                # Build the service for each attempt to avoid stale connections
                sheets_service = build('sheets', 'v4', credentials=credentials)
                
                sheets_service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range='Sheet1!A1',
                    valueInputOption='RAW',
                    body={'values': values}
                ).execute()
                
                logger.info(f"Successfully wrote {len(df)} rows to Google Sheet")
                return
                
            except HttpError as e:
                if retry >= MAX_RETRIES - 1 or e.resp.status not in [429, 500, 502, 503, 504]:
                    raise RuntimeError(f"Failed to write data to Google Sheet: {e}")
                
                wait_time = INITIAL_RETRY_DELAY * (2 ** retry)
                logger.warning(f"Google API error: {e}. Retrying in {wait_time} seconds (attempt {retry+1}/{MAX_RETRIES})")
                time.sleep(wait_time)
            
            except Exception as e:
                raise RuntimeError(f"Failed to write data to Google Sheet: {e}")
    
    # This should never be reached due to the raise in the except block
    raise RuntimeError("Failed to write data to Google Sheet after retries")

def move_sheet_to_folder(
    credentials: Credentials,
    file_id: str,
    folder_id: str
) -> None:
    """Move the Google Sheet to the specified folder."""
    logger.info(f"Moving Google Sheet to folder ID: {folder_id}")
    
    # Standard retry parameters
    MAX_RETRIES = 5
    INITIAL_RETRY_DELAY = 1
    
    for retry in range(MAX_RETRIES):
        try:
            # Build the service
            drive_service = build('drive', 'v3', credentials=credentials)
            
            # Get current parents
            file = drive_service.files().get(
                fileId=file_id, fields='parents'
            ).execute()
            
            previous_parents = ",".join(file.get('parents', []))
            
            # Move file to new folder
            drive_service.files().update(
                fileId=file_id,
                addParents=folder_id,
                removeParents=previous_parents,
                fields='id, parents'
            ).execute()
            
            logger.info("Successfully moved Google Sheet to specified folder")
            return
            
        except HttpError as e:
            if retry >= MAX_RETRIES - 1 or e.resp.status not in [429, 500, 502, 503, 504]:
                raise RuntimeError(f"Failed to move Google Sheet to folder: {e}")
            
            wait_time = INITIAL_RETRY_DELAY * (2 ** retry)
            logger.warning(f"Google API error: {e}. Retrying in {wait_time} seconds (attempt {retry+1}/{MAX_RETRIES})")
            time.sleep(wait_time)
        
        except Exception as e:
            raise RuntimeError(f"Failed to move Google Sheet to folder: {e}")
    
    # This should never be reached due to the raise in the except block
    raise RuntimeError("Failed to move Google Sheet to folder after retries")

def main():
    """Main function to execute the workflow."""
    try:
        # Load configuration
        logger.info("Loading configuration")
        config = load_config()
        
        # Read SQL query from file
        sql_query = read_sql_from_file('trino_MB_query.sql')
        
        # Get Google credentials
        logger.info("Authenticating with Google")
        credentials = get_google_credentials(
            config['google']['client_secret_file'],
            config['google']['token_path']
        )
        
        # Execute Trino query and get results as DataFrame
        df = execute_trino_query(config, sql_query)
        
        # Generate sheet title with today's date
        today = datetime.date.today().strftime('%Y-%m-%d')
        sheet_title = f"{today}- MB Query Export.csv"
        
        # Create Google Sheet
        spreadsheet_id = create_google_sheet(credentials, sheet_title)
        
        # Write data to sheet
        write_dataframe_to_sheet(credentials, spreadsheet_id, df)
        
        # Move sheet to the specified folder
        move_sheet_to_folder(
            credentials,
            spreadsheet_id,
            config['google']['drive_folder_id']
        )
        
        logger.info("Script completed successfully")
        
    except Exception as e:
        logger.error(f"Script failed: {e}")
        raise

if __name__ == "__main__":
    main()
