import json
import requests

def fetch_breweries_data(per_page):
    per_page=200
    url = f"https://api.openbrewerydb.org/breweries?sort=asc&per_page={per_page}"
    breweries_data = []
    page = 1
    
    while True:
        response = requests.get(url + f"&page={page}")
        if response.status_code == 200:
            page_data = response.json()
            if not page_data:
                break  # No more pages available
            breweries_data.extend(page_data)
            page += 1
        else:
            # If the request was unsuccessful, raise an exception
            response.raise_for_status()
    
    return breweries_data

def save_data_to_json(data, filename):
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)

# Fetch data from the API with pagination
breweries_data = fetch_breweries_data(per_page=200)

# Save the data to a JSON file
save_data_to_json(breweries_data, 'all_breweries_data.json')
