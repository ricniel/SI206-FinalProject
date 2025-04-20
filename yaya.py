import sqlite3
import requests
from bs4 import BeautifulSoup

def setup_species_table():
    """Set up the species table in ecoalert.db."""
    conn = sqlite3.connect("ecoalert.db")
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS species (
        species_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE,
        threat_level TEXT,
        population_trend TEXT,
        habitat TEXT,
        location_id TEXT  -- Matches weather table (e.g., '127503')
    )''')
    conn.commit()
    conn.close()

def fetch_iucn_data():
    """Scrape IUCN Red List data, limit to 25 items per run."""
    # Partner implements scraping logic here
    pass

def store_species_data():
    """Store up to 25 species entries in the database."""
    # Partner implements storage logic here, mirroring store_weather_data()
    pass

if __name__ == "__main__":
    setup_species_table()
    store_species_data()