import requests
from datetime import datetime, timedelta
import time

# Constants
GITHUB_API_URL = "https://api.github.com/search/repositories"
OAUTH_TOKEN = "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"  # Replace with your OAuth token
HEADERS = {"Authorization": f"token {OAUTH_TOKEN}"}

# Helper function to fetch repositories for a specific date
def fetch_repositories_for_date(date):
    repositories = []
    page = 1
    while True:
        query = f"q=created:{date}..{date}&sort=updated&order=desc&per_page=100&page={page}"
        response = requests.get(f"{GITHUB_API_URL}?{query}", headers=HEADERS)
        if response.status_code != 200:
            raise Exception(f"Error fetching data: {response.json()}")
        
        data = response.json()
        repositories.extend(data['items'])
        
        if 'next' not in response.links:
            break
        
        page += 1
        print(f"request url: {response.request.url}")
        print(f"repositories fetched: {len(repositories)}")
        time.sleep(2)  # Respect rate limits with a delay between requests
    
    return repositories

# Function to fetch repositories for the past year
def fetch_repositories_for_past_year():
    repositories = []
    today = datetime.utcnow().date()
    one_year_ago = today - timedelta(days=1)
    
    current_date = one_year_ago
    while current_date <= today:
        print(f"Fetching repositories for date: {current_date}")
        repositories.extend(fetch_repositories_for_date(current_date))
        print(f"repositories fetched: {repositories}")
        current_date += timedelta(days=1)
        time.sleep(5)  # Respect rate limits with a delay between requests
    
    return repositories

# Fetch repositories
repositories = fetch_repositories_for_past_year()
print(f"Total repositories fetched: {len(repositories)}")
