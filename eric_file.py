import requests
import sqlite3

API_KEY = "meLAyk4AU2GAy7eXPGxX4AOcJPTmK2DS"
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

def create_database():
    """Set up the weather table in ecoalert.db with proper constraints.
    Inputs: None.
    Outputs: Creates table with integer location_id for joining."""
    conn = sqlite3.connect("ecoalert.db")
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS weather (
        weather_id INTEGER PRIMARY KEY AUTOINCREMENT,
        location_id INTEGER,
        date TEXT,
        temperature REAL,
        humidity REAL,
        precipitation REAL,
        UNIQUE(location_id, date)
    )''')
    conn.commit()
    conn.close()
    
#Grok: to avoid going over hourly limit of 50 requests per hour, add limit = 5 in the parameters
def get_weather(location_key, limit=5):
    """Fetch 5-day weather forecast from AccuWeather API.
    Inputs: location_key (str), limit (int, default 5).
    Outputs: List of forecast dictionaries or empty list if failed."""
    url = f"http://dataservice.accuweather.com/forecasts/v1/daily/5day/{location_key}?apikey={API_KEY}&metric=true"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()["DailyForecasts"][:limit]
        else:
            print(f"Error: {response.status_code} - {response.text}")
            return []
    except:
        print(f"Request failed")
        return []

def store_data():
    """Store up to 25 weather forecast entries in the database.
    Inputs: None.
    Outputs: Integer count of new entries stored."""
    conn = sqlite3.connect('ecoalert.db')
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
                cursor.execute("INSERT OR IGNORE INTO weather (location_id, date, temperature, humidity, precipitation) VALUES (?, ?, ?, ?, ?)", (v, date, item["Temperature"]["Maximum"]["Value"], 50.0, precipitation))
                count += 1
    conn.commit()
    conn.close()
    return count

if __name__ == "__main__":
    create_database()
    store_data()
