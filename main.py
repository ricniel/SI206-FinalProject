from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import sqlite3
import eric_file
import yaya
import json


def join_tables():
    """Merges weather and species tables and prints the results
    Inputs: None
    Outputs: Print results."""
    conn = sqlite3.connect('ecoalert.db')
    cursor = conn.cursor()
    cursor.execute('''
        SELECT DISTINCT w.location_id, w.date, w.temperature, w.precipitation, s.common_name, s.red_list_category
        FROM weather w
        JOIN species s ON w.location_id = s.location_id
        GROUP BY w.location_id, w.date, s.common_name
        LIMIT 25
    ''')
    results = cursor.fetchall()
    print("Location ID | Date       | Temp (°C) | Precip (%) | Species Name       | Red List Status")
    print("-" * 80)
    for row in results:
        #Grok: how do I assign the needed items into variables?
        location_id, date, temp, precip, species_name, red_list = row
        print(f"{location_id:<11} | {date:<10} | {temp:<9.1f} | {precip:<10.1f} | {species_name:<18} | {red_list}")
    conn.close()

def calculate_stats():
    """Calculates the average temperature and species count per location
    Parameters: none
    Inputs: None
    Outputs: Prints a table of the average temperature and species cound per location"""
    conn = sqlite3.connect('ecoalert.db')
    cursor = conn.cursor()
    cursor.execute('''
        SELECT w.location_id, AVG(w.temperature) as avg_temp, COUNT(s.id) as species_count
        FROM weather w
        LEFT JOIN species s ON w.location_id = s.location_id
        GROUP BY w.location_id
    ''')
    results = cursor.fetchall()
    print("Location ID | Avg Temp (°C) | Species Count")
    print("-" * 40)
    for row in results:
        location_id, avg_temp, species_count = row
        print(f"{location_id:<11} | {avg_temp:<13.1f} | {species_count}")
    conn.close()

if __name__ == "__main__":
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    eric_file.create_database()
    yaya.set_up_tables()
    weather_count = eric_file.store_data()
    noaa_regions = ['globe', 'africa', 'europe', 'gulfOfAmerica']
    noaa_data = yaya.scrape_noaa_data(noaa_regions, driver)
    with open("noaa_data.json", "w") as f:
        json.dump(noaa_data, f)
    noaa_count = yaya.store_noaa_data(noaa_data)
    iucn_url = 'https://www.iucnredlist.org/search/list?query=&searchType=species&threats=11'
    iucn_soup = yaya.setup_iucn_webpage_for_scraping(iucn_url, driver)
    iucn_data = yaya.scrape_page_into_dict(iucn_soup)
    with open("iucn_data.json", "w") as f:
        json.dump(iucn_data, f)
    iucn_count = yaya.store_iucn_data(iucn_data)
    join_tables()
    calculate_stats()