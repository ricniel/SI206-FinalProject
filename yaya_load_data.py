from bs4 import BeautifulSoup
import os
import json
import sqlite3
import time

def noaa_region_table(data, cur, conn):
    """
    Creates NOAA region table

    Parameters
    ----------------------------
    data: list
        same regions list as above

    Returns
    ----------------------------
    Nothing
    """
    cur.execute('CREATE TABLE IF NOT EXISTS noaa_regions (id INTEGER PRIMARY KEY, region TEXT)')

    for i in range(len(data)):
        cur.execute("INSERT OR IGNORE INTO noaa_regions (id, region) VALUES (?, ?)", (i, data[i]))
        
    conn.commit()

def noaa_yearly_table(data, cur, conn):
    """
    Takes NOAA region, year, and temp data and puts it into a table

    Parameters
    ----------------------------
    data: list of tuples 
        scrape_noaa_data output

    Returns
    ----------------------------
    Nothing
    """
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

def set_up_iucn_database(db_name):
    """
    Sets up database

    Parameters
    ----------------------------
    db_name: string
        database name

    Returns
    ----------------------------
    Nothing
    """
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + "/" + db_name)
    cur = conn.cursor()
    return cur, conn
    pass

def set_up_iucn_species_table(data, cur, conn):
    """
    Sets up the species table in the database using the dictionary 

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
        "CREATE TABLE IF NOT EXISTS species (id INTEGER PRIMARY KEY, species TEXT, common_name TEXT, population_status TEXT, red_list_category TEXT, geographical_scope TEXT, geo_key INT)"
    )
    for species_info in data.values():
        location = species_info['Location']
        if location == 'Global':
            species_info['Location Key'] = 0
        elif location == 'Europe':
            species_info['Location Key'] = 3
        elif location == 'Gulf of Mexico':
            species_info['Location Key'] = 7
        elif 'Africa' in location:
            species_info['Location Key'] = 1
        else:
            species_info['Location Key'] = 0

    #CODE BELOW WRITTEN WITH HELP FROM CHATGPT
    cur.execute("SELECT MAX(id) FROM species")
    result = cur.fetchone()
    current_max_id = result[0] if result[0] is not None else 0

    start_index = current_max_id
    species_items = list(data.items())
    batch = species_items[start_index:start_index + 25]

    for i, (sci_name, species_info) in enumerate(batch, start=start_index + 1):
        cur.execute("""INSERT OR IGNORE INTO species 
            (id, species, common_name, population_status, red_list_category, geographical_scope, geo_key) 
            VALUES (?, ?, ?, ?, ?, ?, ?)""", 
            (i, sci_name, species_info['Common Name'], species_info['Population Status'], 
            species_info['Red List Category'], species_info['Location'], species_info['Location Key']))


    conn.commit()
    print(f"Inserted rows {start_index + 1} to {start_index + len(batch)}")
    #END OF CODE WRITTEN WITH HELP FROM CHATGPT

    pass


#RUN CODE
noaa_regions = [
    'globe',
    'africa',
    'europe',
    'gulfOfAmerica',
]

with open("noaa_data.json") as f:
    noaa_data = json.load(f)

with open("iucn_data.json") as f:
    iucn_data = json.load(f)

cur, conn = set_up_iucn_database('iucn.db')
noaa_region_table(noaa_regions, cur, conn)
noaa_yearly_table(noaa_data, cur, conn)

set_up_iucn_species_table(iucn_data, cur, conn)