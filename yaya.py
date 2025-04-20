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

def setup_webpage_for_scraping(url):
    driver = Safari()
    # navigate to the target webpage
    driver.get(url)

    time.sleep(5)

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

def set_up_land_region_table(data, cur, conn):
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

    for i in range(len(data)):
        cur.execute("")

    conn.commit()

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
url = 'https://www.iucnredlist.org/search/list?query=&searchType=species&threats=11'
soup = setup_webpage_for_scraping(url)
scrape_page_into_dict(soup)
set_up_iucn_database('iucn.db')