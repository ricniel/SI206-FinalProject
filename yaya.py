from bs4 import BeautifulSoup
import re
import os
import csv
import unittest
import requests
import sqlite3
import time

# LOAD JAVASCRIPT ON WEBPAGE CODE FROM: https://www.codecademy.com/article/web-scrape-with-selenium-and-beautiful-soup with help refining using ChatGPT

# import necessary tools from the selenium library
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver import Safari
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def scrape_noaa_data(region_list):
    driver = Safari()

    temp_anomaly_list = []
    for region in region_list:
        url = f'https://www.ncei.noaa.gov/access/monitoring/climate-at-a-glance/global/time-series/{region}/tavg/land_ocean/12/12/1850-2024'

        driver.get(url)

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        table = soup.find_all('tr', role='row')
        for row in table[1:]:
            year = row.find('a').text
            anomaly = row.find_all('td')[1].get('data-sortval')
            temp_anomaly_list.append((region, year, anomaly))
            
    return temp_anomaly_list

    pass

def noaa_region_table(data, cur, conn):
    cur.execute('CREATE TABLE IF NOT EXISTS noaa_regions (id INTEGER KEY, region TEXT)')

    for i in range(len(data)):
        cur.execute("INSERT OR IGNORE INTO noaa_regions (id, region) VALUES (?, ?)", (i, data[i]))
        
    conn.commit()

def noaa_yearly_table(data, cur, conn):
    cur.execute('CREATE TABLE IF NOT EXISTS noaa_yearly_data (id INTEGER PRIMARY KEY, region_id INTEGER, year TEXT, anomaly NUMERIC)')

    region_map = {}
    for row in cur.execute("SELECT id, region FROM noaa_regions"):
        region_map[row[1].lower()] = row[0] 
    
    transformed_data = []
    for region_name, year, anomaly in data:
        region_id = region_map.get(region_name.lower())
        transformed_data.append((region_id, year, anomaly))

    #CODE BELOW WRITTEN WITH HELP FROM CHATGPT
    cur.execute("SELECT MAX(id) FROM noaa_yearly_data")
    result = cur.fetchone()
    current_max_id = result[0] if result[0] is not None else 0

    start_index = current_max_id
    batch = transformed_data[start_index:start_index + 25]

    for i, region in enumerate(batch, start=start_index + 1):
            cur.execute("INSERT OR IGNORE INTO noaa_yearly_data (id, region_id, year, anomaly) VALUES (?, ?, ?, ?)", (i, region[0], region[1], region[2]))

    conn.commit()
    print(f"Inserted rows {start_index + 1} to {start_index + len(batch)}")
    #END OF CODE WRITTEN WITH HELP FROM CHATGPT

def setup_iucn_webpage_for_scraping(url):
    driver = Safari()
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
    # while True:
    #     try: 
    #         show_more = driver.find_element(By.CLASS_NAME, "section__link-out")

    #         show_more.click()

    #         time.sleep(2)
    #     except:
    #         print("No more 'Show More' button found or it's not clickable anymore.")
    #         break

    soup = BeautifulSoup(driver.page_source, 'html.parser')

    return soup

# def set_up_noaa_webpage_for_scraping(url):
#     resp = requests.get(url)
#     soup = BeautifulSoup(resp.content, 'html.parser')
    
#     return soup
#     pass

# def scrape_land_region(url):
#     soup = setup_webpage_for_scraping(url)


def scrape_page_into_dict(soup):

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
        print(location_list)

        species_dict['Common Name'] = common_name
        species_dict['Population Status'] = pop_status
        species_dict['Red List Category'] = species_status
        species_dict['Location'] = location_list

        iucn_dict[sci_name] = species_dict

    return(iucn_dict)


def set_up_iucn_database(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + "/" + db_name)
    cur = conn.cursor()
    return cur, conn
    pass

def set_up_regional_assessment_table(data, cur, conn):
    """
    Sets up the land region table in the database

    Parameters
    ----------------------------
    data: list
        list of land regions as defined by the IUCN
    cur: cursor
        The database cursor object

    conn: connection
        The database connection object
    """

    cur.execute("CREATE TABLE IF NOT EXISTS geographical_scope (id INTEGER PRIMARY KEY, region TEXT)")

    batch_size = 25
    start_id = 1

    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        for j, item in enumerate(batch):
            cur.execute("INSERT INTO geographical_scope (id, region) VALUES (?, ?)", (start_id + j, item))
        
        conn.commit()
        print(f"Inserted rows {i+1} to {i+len(batch)}")

    pass

def set_up_tables(data, cur, conn):
    """
    Sets up the tables in the database using the dictionary 

    Parameters
    ----------------------------
    data: nested dictionary 
        scientific species name as key and 
    cur: cursor
        The database cursor object

    conn: connection
        The database connection object
    """

    cur.execute(
        "CREATE TABLE IF NOT EXISTS species (id INTEGER PRIMARY KEY, common_name TEXT, population_status TEXT, red_list_category TEXT, geographical_scope INT, land_region2 INT, land_region3 INT)"
    )

    pass


#RUN CODE
noaa_regions = [
    'globe',
    'africa',
    'asia',
    'europe',
    'northAmerica',
    'oceania',
    'southAmerica',
    'gulfOfAmerica',
    'arctic',
    'antarctic'
]

iucn_regions = [
    'Global',
    'Europe',
    'Mediterranean',
    'Pan-Africa',
    'Central Africa',
    'Eastern Africa',
    'Western Africa',
    'Gulf of Mexico',
    'S. Africa FW',
    'Persian Gulf',
    'Northern Africa',
    'Northeastern Africa',
    'Carribbean',
    'Arabian Sea'
]

noaa_data = scrape_noaa_data(noaa_regions)

#iucn_url = 'https://www.iucnredlist.org/search/list?query=&searchType=species&threats=11'

#iucn_soup = setup_iucn_webpage_for_scraping(iucn_url)
#scrape_page_into_dict(iucn_soup)

cur, conn = set_up_iucn_database('iucn.db')
noaa_region_table(noaa_regions, cur, conn)
noaa_yearly_table(noaa_data, cur, conn)


#set_up_regional_assessment_table(iucn_regions, cur, conn)


