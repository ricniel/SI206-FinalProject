import requests
import json
import re
import sqlite3

API_KEY = "meLAyk4AU2GAy7eXPGxX4AOcJPTmK2DS"

def create_database():
    conn = sqlite3.connect("ecoalert.db")
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS weather (
        weather_id INTEGER PRIMARY KEY AUTOINCREMENT,
        location_id TEXT,
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