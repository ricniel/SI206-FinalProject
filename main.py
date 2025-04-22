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
        WITH RankedSpecies AS (
            SELECT s.common_name, s.red_list_category, s.location_id,
                   w.date, w.temperature, w.precipitation,
                   ROW_NUMBER() OVER (PARTITION BY s.common_name ORDER BY w.date DESC) as rn
            FROM species s
            JOIN weather w ON s.location_id = w.location_id
        )
        SELECT DISTINCT location_id, date, temperature, precipitation, common_name, red_list_category
        FROM RankedSpecies
        WHERE rn = 1
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
        SELECT w.location_id, 
               AVG(w.temperature) as avg_temp, 
               (SELECT COUNT(DISTINCT common_name) 
                FROM species s 
                WHERE s.location_id = w.location_id) as species_count,
               (SELECT AVG(temp_anomaly) 
                FROM climate c 
                WHERE c.location_id = w.location_id 
                AND c.year >= 2020) as recent_temp_anomaly,
               AVG(wd.wind_speed) as avg_wind_speed
        FROM weather w
        LEFT JOIN weather_details wd ON w.location_id = wd.location_id AND w.date = wd.date
        GROUP BY w.location_id
    ''')
    results = cursor.fetchall()
    with open("calculations.txt", "w") as file:
        #Grok: how should I format the file write? Hinted me at these two .writes
        file.write("Location ID | Avg Temp (°C) | Species Count | Recent Temp Anomaly (°C) | Avg Wind Speed (km/h)\n")
        file.write("-" * 90 + "\n")
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