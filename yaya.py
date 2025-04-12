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
from selenium.webdriver import Safari

# set up chrome driver
driver = Safari()

# navigate to the target webpage
driver.get("https://www.iucnredlist.org/search/list?query=&searchType=species")

time.sleep(5)

soup = BeautifulSoup(driver.page_source, 'html.parser')

species_card_list = soup.find_all('li', class_='list-results__item')

species_dict = {}

for card in species_card_list:
    sci_name = soup.find_all('p', class_="list-results__subtitle")
    



# page = requests.get(url)

# soup = BeautifulSoup(page.text, 'html.parser')


# #get every species card 

# def get_species_card():
#     
#     return species_card_list


# #TEST CASES
# class TestCases(unittest.TestCase):
#     def test_species_card_list(self):
#         self.assertEqual(type(get_species_card), list)


# def main():
#     get_species_card()

# if __name__ == "__main__":
#     unittest.main(verbosity=2) 
    # main() 