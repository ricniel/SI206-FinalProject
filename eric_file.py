import requests
import sqlite3
import json
import os
import time

API_KEY = "	NpqJSgIjxipbcSLLe8NnvVorarnsZ2gh"
#After looking at the rubric and debugging, we figured out that we needed shared integer keys
#However, IUCN and the accuweather API both have different keys
#With the help of Claude AI, I created this location map for the database.
LOCATION_MAP = {
    "127503": 1,   # Antananarivo, Madagascar
    "305343": 2,   # Sao Paulo, Brazil
    "264120": 3,   # Sydney, Australia
    "349727": 4,   # Cape Town, South Africa
    "307297": 5,   # Buenos Aires, Argentina
    "314929": 6,   # Jakarta, Indonesia
    "226396": 7,   # Mumbai, India
    "202396": 8,   # Nairobi, Kenya
    "297442": 9,   # Lima, Peru
    "324505": 10,  # Bangkok, Thailand
    "300597": 11,  # Bogotá, Colombia
    "215854": 12,  # Kuala Lumpur, Malaysia
    "312114": 13,  # Manila, Philippines
    "281184": 14,  # San José, Costa Rica
    "252316": 15,  # Auckland, New Zealand
    "332468": 16,  # Hanoi, Vietnam
    "224689": 17,  # Delhi, India
    "249404": 18,  # Santiago, Chile
    "260622": 19,  # Melbourne, Australia
    "208971": 20   # Accra, Ghana
}
API_CALL_COUNT = 0
def create_database():
    """Set up the weather table in ecoalert.db with proper constraints.
    Inputs: None.
    Outputs: Creates table with integer location_id for joining."""
    conn = sqlite3.connect("ecoalert.db")
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS weather (
        weather_id INTEGER PRIMARY KEY AUTOINCREMENT,
        location_id INTEGER,
        date TEXT,
        temperature REAL,
        humidity REAL,
        precipitation REAL,
        UNIQUE(location_id, date)
    )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS weather_details (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        location_id INTEGER,
        date TEXT,
        wind_speed REAL,
        UNIQUE(location_id, date)
    )''')
    conn.commit()
    conn.close()

def load_weather_cache():
    """Load cached weather data from weather_cache.json.
    Inputs: None.
    Outputs: Dictionary of cached weather data."""
    cache_file = "weather_cache.json"
    if os.path.exists(cache_file):
        with open(cache_file, "r") as file:
            try:
                return json.load(file)
            except:
                return {}
    return {}

def save_weather_cache(cache):
    """Save weather data to weather_cache.json.
    Inputs: cache (dict) - Weather data to save.
    Outputs: None."""
    with open("weather_cache.json", "w") as f:
        json.dump(cache, f)

    
#Grok: to avoid going over hourly limit of 50 requests per hour, add limit = 5 in the parameters, max_retries, retry_delay, max_apie calls
def get_weather(location_key, limit=5, max_retries=3, retry_delay=3600, max_api_calls=5):
    """Fetch 5-day weather forecast from AccuWeather API with caching and retry logic.
    Inputs:
        location_key (str): Location key for the API.
        limit (int): Number of forecast days to return (default 5).
        max_retries (int): Maximum number of retry attempts if rate limit is hit.
        retry_delay (int): Delay in seconds between retries (default 1 hour).
        max_api_calls (int): Maximum number of API calls allowed in this run.
    Outputs:
        List of forecast dictionaries or empty list if failed or limit reached."""
    #grok: why is API call count not showing up? told me to use global
    global API_CALL_COUNT
    cache = load_weather_cache()
    cache_key = f"{location_key}_{limit}"
    if cache_key in cache:
        print(f"Using cached weather data for location {location_key}")
        return cache[cache_key]
    if API_CALL_COUNT >= max_api_calls:
        print(f"API call limit ({max_api_calls}) reached. Skipping API request for location {location_key}.")
        return []
    url = f"http://dataservice.accuweather.com/forecasts/v1/daily/5day/{location_key}?apikey={API_KEY}&metric=true"
    for attempt in range(max_retries + 1):
        if API_CALL_COUNT >= max_api_calls:
            print(f"API call limit ({max_api_calls}) reached during retries for location {location_key}.")
            return []
        try:
            API_CALL_COUNT += 1
            print(f"Making API request {API_CALL_COUNT}/{max_api_calls} for location {location_key}")
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()["DailyForecasts"][:limit]
                # Cache the data
                cache[cache_key] = data
                save_weather_cache(cache)
                return data
            elif response.status_code == 503:
                print(f"Error: {response.status_code} - {response.text}")
                if attempt < max_retries:
                    print(f"Rate limit exceeded. Retrying in {retry_delay // 60} minutes (attempt {attempt + 1}/{max_retries})...")
                    time.sleep(retry_delay)
                else:
                    print("Max retries reached. Please wait for the rate limit to reset or use a different API key.")
                    return []
            else:
                print("Error")
                return []
        except:
            print("Request failed")
            return []
    return []

def store_data(max_api_calls = 5):
    """Store up to 25 weather forecast entries in the database.
    Inputs: None.
    Outputs: Integer count of new entries stored."""
    conn = sqlite3.connect('ecoalert.db')
    global API_CALL_COUNT
    API_CALL_COUNT = 0
    cursor = conn.cursor()
    count = 0
    for k, v in LOCATION_MAP.items():
        if count >= 25:
            break
        weather = get_weather(k)
        for item in weather:
            if count >= 25:
                break
            #Grok: how do I pull the correct date format?
            date = item["Date"][:10]
            cursor.execute("SELECT COUNT(*) FROM weather WHERE location_id=? AND date=?", (v, date))
            #Claude: What cursor.fetchone line could I write to ensure no duplicates?
            if cursor.fetchone()[0] == 0:
                #Grok: to fix a key error, add a default precipitation value
                precipitation = 50.0 if item["Day"].get("HasPrecipitation", False) else 0.0
                #Note: Due to using Accuweather's free API, humidity has been hardcoded to 50.
                cursor.execute("INSERT OR IGNORE INTO weather (location_id, date, temperature, humidity, precipitation) VALUES (?, ?, ?, ?, ?)", (v, date, item["Temperature"]["Maximum"]["Value"], 50.0, precipitation))
                count += 1
    conn.commit()
    conn.close()
    return count

if __name__ == "__main__":
    create_database()
    store_data()
