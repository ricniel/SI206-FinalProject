from bs4 import BeautifulSoup
import re
import os
import csv
import unittest
import requests
import sqlite3
import time

# LOAD JAVASCRIPT ON WEBPAGE CODE FROM:

# import necessary tools from the selenium library
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver import Safari

# set up chrome driver
driver = Safari()

# navigate to the target webpage
driver.get("https://www.iucnredlist.org/search/list?query=&searchType=species")

time.sleep(5)


# while True:
#     try: 
#         show_more = driver.find_element(By.CLASS_NAME, "section__link-out")

#         show_more.click()

#         time.sleep(2)
#     except:
#         print("No more 'Show More' button found or it's not clickable anymore.")
#         break

soup = BeautifulSoup(driver.page_source, 'html.parser')

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

print(iucn_dict)
