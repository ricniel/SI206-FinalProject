from bs4 import BeautifulSoup
import os
import json
import sqlite3

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

def population_status_table(data, cur, conn):

    cur.execute('CREATE TABLE IF NOT EXISTS population_status (id INTEGER PRIMARY KEY, population_status TEXT)')

    for i in range(len(data)):
        cur.execute('INSERT OR IGNORE INTO population_status (id, population_status) VALUES (?, ?)', (i, data[i]))

    conn.commit()

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

def set_up_iucn_red_list_table(data, cur, conn):
    """
    Sets up the red list category table in the database using list of categories

    Parameters
    ----------------------------
    data: list
        red list categories
    cur: cursor
        The database cursor object

    conn: connection
        The database connection object
    """
    cur.execute("CREATE TABLE IF NOT EXISTS red_list_cat (id INTEGER PRIMARY KEY, red_list_category TEXT)")

    for i in range(len(data)):
        cur.execute("INSERT OR IGNORE INTO red_list_cat (id, red_list_category) VALUES (?,?)", (i, data[i]))
    
    conn.commit()


def set_up_iucn_species_table(data, red_list_categories, noaa_regions, population_status, cur, conn):
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
        "CREATE TABLE IF NOT EXISTS species (id INTEGER PRIMARY KEY, species TEXT, common_name TEXT, population_status_key INT, geo_key INT, red_list_key INT)"
    )

    # HELP FROM CHATGPT TO ASSIGN KEYS 
    location_map = {region.lower(): idx for idx, region in enumerate(noaa_regions)}
    red_list_map = {cat: idx for idx, cat in enumerate(red_list_categories)}
    population_status_map = {cat: idx for idx, cat in enumerate(population_status)}

    for species_info in data.values():
        # Normalize location string
        loc = species_info['Location'].lower().replace(' ', '')
        # Find a matching NOAA region key (default to 'globe' or index 0)
        matched = next((key for key in location_map if key in loc), 'globe')
        species_info['Location Key'] = location_map.get(matched, 0)

        # Red List key based on category index (default to 0 if not found)
        red_list_cat = species_info['Red List Category']
        species_info['Red List Key'] = red_list_map.get(red_list_cat, 0)

        pop_status = species_info.get('Population Status', '')
        species_info['Population Status Key'] = population_status_map.get(pop_status, 0)


    #CODE BELOW WRITTEN WITH HELP FROM CHATGPT
    cur.execute("SELECT MAX(id) FROM species")
    result = cur.fetchone()
    current_max_id = result[0] if result[0] is not None else 0

    start_index = current_max_id
    species_items = list(data.items())
    batch = species_items[start_index:start_index + 25]

    for i, (sci_name, species_info) in enumerate(batch, start=start_index + 1):
        cur.execute("""INSERT OR IGNORE INTO species 
            (id, species, common_name, population_status_key, geo_key, red_list_key) 
            VALUES (?, ?, ?, ?, ?, ?)""", 
            (i, sci_name, species_info['Common Name'], species_info['Population Status Key'], species_info['Location Key'], species_info['Red List Key']))


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

red_list_categories = [
    'Not Evaluated',
    'Data Deficient',
    'Least Concern',
    'Near Threatened',
    'Vulnerable',
    'Endangered',
    'Critically Endangered',
    'Extinct in the Wild',
    'Extinct'
]

population_status = [
    'Unknown', 
    'Stable',
    'Decreasing',
    'Increasing'
]

with open("noaa_data.json") as f:
    noaa_data = json.load(f)

with open("iucn_data.json") as f:
    iucn_data = json.load(f)

print(len(iucn_data))

cur, conn = set_up_iucn_database('iucn.db')
noaa_region_table(noaa_regions, cur, conn)
noaa_yearly_table(noaa_data, cur, conn)

set_up_iucn_red_list_table(red_list_categories, cur, conn)
population_status_table(population_status, cur, conn)

set_up_iucn_species_table(iucn_data, red_list_categories, noaa_regions, population_status, cur, conn)