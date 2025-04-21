from bs4 import BeautifulSoup
import os
import json
import sqlite3
import time

# LOAD JAVASCRIPT ON WEBPAGE CODE FROM: https://www.codecademy.com/article/web-scrape-with-selenium-and-beautiful-soup with help refining using ChatGPT

# import necessary tools from the selenium library
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver import Safari
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

LOCATION_IDS = {
    "Antananarivo": 1,   # Madagascar
    "Sao Paulo": 2,      # Brazil
    "Sydney": 3,         # Australia
    "Cape Town": 4,      # South Africa
    "Buenos Aires": 5,   # Argentina
    "Jakarta": 6,        # Indonesia
    "Mumbai": 7,         # India
    "Nairobi": 8,        # Kenya
    "Lima": 9,           # Peru
    "Bangkok": 10,       # Thailand
    "Bogotá": 11,        # Colombia
    "Kuala Lumpur": 12,  # Malaysia
    "Manila": 13,        # Philippines
    "San José": 14,      # Costa Rica
    "Auckland": 15,      # New Zealand
    "Hanoi": 16,         # Vietnam
    "Delhi": 17,         # India
    "Santiago": 18,      # Chile
    "Melbourne": 19,     # Australia
    "Accra": 20          # Ghana
}
NOAA_REGIONS = {
    "globe": ["Antananarivo", "Sao Paulo", "Sydney", "Cape Town", "Buenos Aires", "Jakarta", "Mumbai", "Nairobi", "Lima", "Bangkok", "Bogotá", "Kuala Lumpur", "Manila", "San José", "Auckland", "Hanoi", "Delhi", "Santiago", "Melbourne", "Accra"],
    "africa": ["Cape Town", "Nairobi", "Accra"],
    "europe": [],  
    "gulfOfAmerica": ["Sao Paulo", "Buenos Aires", "Lima", "Bogotá", "Santiago"]
}
IUCN_REGION_MAP = {
    "Global": NOAA_REGIONS["globe"],
    "Africa": NOAA_REGIONS["africa"], 
    "Northern Africa": NOAA_REGIONS["africa"],
    "Western Africa": NOAA_REGIONS["africa"],
    "Pan-Africa": NOAA_REGIONS["africa"],
    "Mediterranean": NOAA_REGIONS["africa"],  
    "Gulf of America": NOAA_REGIONS["gulfOfAmerica"],
    "Europe": NOAA_REGIONS["europe"], 
    "Persian Gulf": [], 
}
def set_up_tables():
    """
    Sets up the species and climate tables in ecoalert.db

    Parameters
    ----------------------------
    None

    Returns
    ----------------------------
    cur: cursor
        The database cursor object
    conn: connection
        The database connection object
    """
    conn = sqlite3.connect("ecoalert.db")
    cursor = conn.cursor()
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS species (id INTEGER PRIMARY KEY AUTOINCREMENT, common_name TEXT, population_status TEXT, red_list_category TEXT, location_id INTEGER, UNIQUE(common_name, location_id))"
    )
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS climate (id INTEGER PRIMARY KEY AUTOINCREMENT, region TEXT, year INTEGER, temp_anomaly REAL, location_id INTEGER, UNIQUE(region, year, location_id))"
    )
    conn.commit()
    return cursor, conn

def scrape_noaa_data(region_list, driver):
    """
    Scrapes NOAA site for avg yearly land and ocean temp going back 100 years

    Parameters
    ----------------------------
    region_list: list
        list of land regions NOAA can be filtered by

    Returns
    ----------------------------
    temp_anomaly_list: list of tuples
        each tuple consists of region, year, and avg temp anomaly 
    """

    temp_anomaly_list = []
    for region in region_list:
        url = f'https://www.ncei.noaa.gov/access/monitoring/climate-at-a-glance/global/time-series/{region}/tavg/land_ocean/12/12/1924-2024'
        driver.get(url)
        time.sleep(2)

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        table = soup.find_all('tr', role='row')
        for row in table[1:]:
            year = row.find('a').text
            anomaly = row.find_all('td')[1].get('data-sortval')
            temp_anomaly_list.append((region, year, anomaly))
            
    return temp_anomaly_list

def store_noaa_data(noaa_data):
    """
    Stores NOAA data into the climate table, up to 25 entries

    Parameters
    ----------------------------
    noaa_data: list of tuples
        region, year, and temp anomaly data

    Returns
    ----------------------------
    Integer: number of new entries stored
    """
    cursor, conn = set_up_tables()
    count = 0
    for region, year, anomaly in noaa_data:
        if count >= 25:
            break
        locations = NOAA_REGIONS.get(region, [])
        for location in locations:
            location_id = LOCATION_IDS.get(location)
            if not location_id:
                continue
            cursor.execute("SELECT COUNT(*) FROM climate WHERE region=? AND year=? AND location_id=?", (region, year, location_id))
            if cursor.fetchone()[0] == 0:
                cursor.execute("INSERT OR IGNORE INTO climate (region, year, temp_anomaly, location_id) VALUES (?, ?, ?, ?)",
                            (region, year, float(anomaly), location_id))
                count += 1
                print(f"Stored NOAA data: Region {region}, Year {year}, Location ID {location_id}")
    conn.commit()
    conn.close()
    print(f"Total NOAA entries stored: {count}")
    return count
    

def setup_iucn_webpage_for_scraping(url, driver):
    """
    Uses BS to take IUCN url to soup

    Parameters
    ----------------------------
    url: string
        IUCN url

    Returns
    ----------------------------
    soup
    """
    # navigate to the target webpage
    driver.get(url)

    # Wait for the "Remove" link to be clickable and click it
    try:
        wait = WebDriverWait(driver, 10)
        
        # Find the 'Remove' link by text
        remove_btn = wait.until(EC.element_to_be_clickable(
            (By.XPATH, '//a[text()="Remove"]')
        ))
        
        remove_btn.click()
        time.sleep(3)  # Wait for results to update
        print("✅ 'Global' tag removed.")
    except Exception as e:
        print("⚠️ Could not find or click 'Remove':", e)

    # loads press more button 
    while True:
        try: 
            show_more = driver.find_element(By.CLASS_NAME, "section__link-out")

            show_more.click()

            time.sleep(2)
        except:
            print("No more 'Show More' button found or it's not clickable anymore.")
            break

    soup = BeautifulSoup(driver.page_source, 'html.parser')

    return soup

def scrape_page_into_dict(soup):
    """
    Scrapes IUCN soup to get dictionary for each species 

    Parameters
    ----------------------------
    soup: IUCN page content

    Returns
    ----------------------------
    nested dict:
        information on each species 
    """

    species_card_list = soup.find_all('li', class_='list-results__item')

    iucn_dict = {}

    for card in species_card_list:
        species_dict = {}
        #print(f"card:{card}")
        common_name = card.find('h2', class_='list-results__title').text.strip()
        print(f'common name: {common_name}')

        subtitle_tag = card.find_all('p', class_='list-results__subtitle')
        for p_tag in subtitle_tag:
            sci_name = p_tag.text.strip()
            print(sci_name)
        
        pop_status = card.find('span', class_='species-population').text.strip()
        print(pop_status)

        cat_tag = card.find('a', class_='species-category')
        species_status = cat_tag['title']
        print(species_status)

        location = card.find('span', class_='species-assessment').text.strip()
        location_list = location.split(', ')
        location_str = location_list[0]

        species_dict['Common Name'] = common_name
        species_dict['Population Status'] = pop_status
        species_dict['Red List Category'] = species_status
        species_dict['Location'] = location_str

        iucn_dict[sci_name] = species_dict

    return iucn_dict

def store_iucn_data(iucn_data):
    """
    Stores IUCN data into the species table, up to 25 entries

    Parameters
    ----------------------------
    iucn_data: nested dict
        species data from IUCN

    Returns
    ----------------------------
    Integer: number of new entries stored
    """
    cursor, conn = set_up_tables()
    count = 0
    for sci_name, species_dict in iucn_data.items():
        if count >= 25:
            break
        common_name = species_dict['Common Name']
        pop_status = species_dict['Population Status']
        red_list_category = species_dict['Red List Category']
        location_str = species_dict['Location']
        locations = IUCN_REGION_MAP.get(location_str, [])
        if not locations:
            for k, v in LOCATION_IDS.items():
                if location_str in k:
                    locations = [k]
                    break
            if not locations:
                print(f"Could not map location {location_str} to a location_id, skipping.")
                continue
        for location in locations:
            if count >= 25:
                break
            location_id = LOCATION_IDS.get(location)
            if not location_id:
                print(f"Could not map location {location} to a location_id, skipping.")
                continue

            cursor.execute("SELECT COUNT(*) FROM species WHERE common_name=? AND location_id=?", (common_name, location_id))
            if cursor.fetchone()[0] == 0:
                cursor.execute("INSERT OR IGNORE INTO species (common_name, population_status, red_list_category, location_id) VALUES (?, ?, ?, ?)",
                            (common_name, pop_status, red_list_category, location_id))
                count += 1
                print(f"Stored IUCN species: {common_name} for Location ID {location_id}")
    conn.commit()
    conn.close()
    print(f"Total IUCN species entries stored: {count}")
    return count

noaa_regions = [
    'globe',
    'africa',
    'europe',
    'gulfOfAmerica',
]

driver = webdriver.Chrome() 

noaa_data = scrape_noaa_data(noaa_regions, driver)
with open("noaa_data.json", "w") as f:
    json.dump(noaa_data, f)

iucn_url = 'https://www.iucnredlist.org/search/list?query=&searchType=species&threats=11'
iucn_soup = setup_iucn_webpage_for_scraping(iucn_url, driver)
iucn_data = scrape_page_into_dict(iucn_soup)
with open("iucn_data.json", "w") as f:
    json.dump(iucn_data, f)
