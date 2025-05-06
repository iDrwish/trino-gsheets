# Lightweight Trino to Google Sheets Exporter

Frustrated from lack of connectors for Trino, so simple Python script to execute a SQL query against a Trino database, fetches the results, and exports them to a Google Sheet. Vibe coded by Cursor.

## Features

- Connects to Trino database and executes a SQL query loaded from a file
- Authenticates with Google APIs using OAuth 2.0
- Creates a new Google Sheet with dynamic naming
- error handling and logging

## Prerequisites

- Python 3.11 or higher
- A Google Cloud Project with the Google Sheets API and Google Drive API enabled
- OAuth 2.0 client credentials (client ID and client secret) from Google Cloud Console
- Access to a Trino database
- SQL query file (e.g., trino_MB_query.sql)

## Setup

1. Clone this repository:
   ```
   git clone [repository-url]
   cd trino-gsheets
   ```

2. Create and activate a virtual environment:
   ```
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. Install the required dependencies:
   ```
   pip install -e .
   ```

4. Set up your Google Cloud Project:
   - Go to the [Google Cloud Console](https://console.cloud.google.com/)
   - Create a new project or select an existing one
   - Enable the Google Sheets API and Google Drive API
   - Configure the OAuth consent screen
   - Create OAuth 2.0 client credentials (Desktop application type)
   - Download the client secret JSON file

5. Create a `.env` file based on the provided template:
   ```
   cp env.example .env
   ```

6. Edit the `.env` file with your specific configuration:
   - Trino connection details (host, port, user, catalog, schema)
   - Path to your Google client secret JSON file
   - ID of the target Google Drive folder
   - Path to the SQL query file

7. Create or modify your SQL query file (default: trino_MB_query.sql)

## Usage

Run the script with:

```
python main.py
```

On the first run, the script will open a browser window for you to authenticate with your Google account and grant the necessary permissions. After successful authentication, the token will be saved for future use.

## SQL Query File

The script reads the SQL query from a file specified in the `.env` configuration. This allows you to:
- Maintain complex SQL queries separately from the code
- Version control your SQL queries
- Easily modify queries without changing the Python code

## Authentication Process

This script uses OAuth 2.0 for authentication with Google APIs. When you run the script for the first time:

1. It will open a browser window asking you to log in to your Google account
2. You'll need to grant the requested permissions for Google Sheets and Google Drive
3. After authorization, Google will redirect to a local URL that the script is listening on
4. The script will capture the authorization code from this redirect
5. The authorization code will be exchanged for access and refresh tokens
6. These tokens will be saved locally for future use

## Error Handling

Error handling for various scenarios:
- Missing configuration
- SQL file reading errors
- Trino connection failures
- SQL query execution errors
- Google API authentication failures
- Google Sheets/Drive API errors

Errors are logged with detailed information to help diagnose issues.

## Security Considerations

- The `token.json` file contains sensitive information. Keep it secure and do not commit it to version control.
- Consider adding `token.json` to your `.gitignore` file.
- The OAuth 2.0 client secret should also be kept secure.