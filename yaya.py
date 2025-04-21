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

driver = webdriver.Chrome() 

def scrape_noaa_data(region_list):
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
        url = f'https://www.ncei.noaa.gov/access/monitoring/climate-at-a-glance/global/time-series/{region}/tavg/land_ocean/12/12/1850-2024'

        driver.get(url)

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        table = soup.find_all('tr', role='row')
        for row in table[1:101]:
            year = row.find('a').text
            anomaly = row.find_all('td')[1].get('data-sortval')
            temp_anomaly_list.append((region, year, anomaly))
            
    return temp_anomaly_list

    pass

def setup_iucn_webpage_for_scraping(url):
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

noaa_regions = [
    'globe',
    'africa',
    'europe',
    'gulfOfAmerica',
]
noaa_data = scrape_noaa_data(noaa_regions)
with open("noaa_data.json", "w") as f:
    json.dump(noaa_data, f)

iucn_url = 'https://www.iucnredlist.org/search/list?query=&searchType=species&threats=11'
iucn_soup = setup_iucn_webpage_for_scraping(iucn_url)
iucn_data = scrape_page_into_dict(iucn_soup)
with open("iucn_data.json", "w") as f:
    json.dump(iucn_data, f)
