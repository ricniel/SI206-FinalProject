import sqlite3

def migrate_data():
    """Migrate data from old schema to new schema with normalized tables.
    This function creates new normalized tables for regions, population status,
    red list categories, and species names. It then migrates data from existing
    species and climate tables to new schemas with foreign key relationships,
    drops old tables, and renames new tables to replace them.
    Parameters:
        None
    Returns:
        None
    """
    conn = sqlite3.connect('ecoalert.db')
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(species)")
    columns = [col[1] for col in cursor.fetchall()]
    if 'species_name_key' in columns and 'population_status_key' in columns:
        conn.close()
        return
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS noaa_regions (
        id INTEGER PRIMARY KEY,
        region TEXT UNIQUE
    )''')
    regions = [('globe',), ('africa',), ('europe',), ('gulfOfAmerica',)]
    cursor.executemany('''
    INSERT OR IGNORE INTO noaa_regions (region) VALUES (?)
    ''', regions)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS population_status (
        id INTEGER PRIMARY KEY,
        status TEXT UNIQUE
    )''')
    cursor.execute('''
    SELECT DISTINCT status FROM species
    ''')
    statuses = cursor.fetchall()
    cursor.executemany('''
    INSERT OR IGNORE INTO population_status (status) VALUES (?)
    ''', statuses)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS red_list_cat (
        id INTEGER PRIMARY KEY,
        red_list_category TEXT UNIQUE
    )''')
    cursor.execute('''
    SELECT DISTINCT red_list FROM species
    ''')
    categories = cursor.fetchall()
    cursor.executemany('''
    INSERT OR IGNORE INTO red_list_cat (red_list_category) VALUES (?)
    ''', categories)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS species_names (
        id INTEGER PRIMARY KEY,
        common_name TEXT UNIQUE
    )''')
    cursor.execute('''
    SELECT DISTINCT name FROM species
    ''')
    names = cursor.fetchall()
    cursor.executemany('''
    INSERT OR IGNORE INTO species_names (common_name) VALUES (?)
    ''', names)
    cursor.execute('''
    CREATE TABLE species_new (
        id INTEGER PRIMARY KEY,
        species_name_key INTEGER,
        population_status_key INTEGER,
        red_list_key INTEGER,
        location_id INTEGER,
        FOREIGN KEY (species_name_key) REFERENCES species_names(id),
        FOREIGN KEY (population_status_key) REFERENCES population_status(id),
        FOREIGN KEY (red_list_key) REFERENCES red_list_cat(id)
    )''')
    cursor.execute('''
    CREATE TABLE climate_new (
        id INTEGER PRIMARY KEY,
        geo_key INTEGER,
        year INTEGER,
        temp_anomaly REAL,
        location_id INTEGER,
        FOREIGN KEY (geo_key) REFERENCES noaa_regions(id)
    )''')
    cursor.execute('''
    INSERT INTO species_new (id, species_name_key, population_status_key, red_list_key, location_id)
    SELECT 
        s.id,
        (SELECT id FROM species_names WHERE common_name = s.name),
        (SELECT id FROM population_status WHERE status = s.status),
        (SELECT id FROM red_list_cat WHERE red_list_category = s.red_list),
        s.location_id
    FROM species s
    ''')
    cursor.execute('''
    INSERT INTO climate_new (id, geo_key, year, temp_anomaly, location_id)
    SELECT 
        c.id,
        (SELECT id FROM noaa_regions WHERE region = c.region),
        c.year,
        c.temp_anomaly,
        c.location_id
    FROM climate c
    ''')
    cursor.execute("DROP TABLE IF EXISTS species")
    cursor.execute("DROP TABLE IF EXISTS climate")
    cursor.execute("ALTER TABLE species_new RENAME TO species")
    cursor.execute("ALTER TABLE climate_new RENAME TO climate") 
    conn.commit()
    conn.close()

def update_join_tables():
    """Retrieve recent species and weather data with joins.
    This function performs a complex join across species, species_names,
    red_list_cat, and weather tables to get the most recent weather data
    for each species, limited to 25 distinct records.
    Parameters: None
    Returns:
        list: A list of tuples containing location_id, date, temperature,
              precipitation, common_name, and red_list_category for each record.
    """
    conn = sqlite3.connect('ecoalert.db')
    cursor = conn.cursor()
    cursor.execute('''
        WITH RankedSpecies AS (
            SELECT sn.common_name, rlc.red_list_category, s.location_id,
                   w.date, w.temperature, w.precipitation,
                   ROW_NUMBER() OVER (PARTITION BY sn.common_name ORDER BY w.date DESC) as rn
            FROM species s
            JOIN species_names sn ON s.species_name_key = sn.id
            JOIN red_list_cat rlc ON s.red_list_key = rlc.id
            JOIN weather w ON s.location_id = w.location_id
            WHERE sn.common_name IS NOT NULL AND sn.common_name != ''
        )
        SELECT DISTINCT location_id, date, temperature, precipitation, common_name, red_list_category
        FROM RankedSpecies
        WHERE rn = 1
        LIMIT 25
    ''')
    results = cursor.fetchall()
    conn.close()
    return results
def update_calculate_stats():
    """Calculate statistical summaries by location.
    This function aggregates weather and species data to compute average
    temperature, species count, recent temperature anomaly, and average
    wind speed for each location.
    Parameters: None
    Returns: list: A list of tuples containing location_id, avg_temp, species_count,
              recent_temp_anomaly, and avg_wind_speed for each location.
    """
    conn = sqlite3.connect('ecoalert.db')
    cursor = conn.cursor()
    cursor.execute('''
        SELECT w.location_id, 
               AVG(w.temperature) as avg_temp, 
               (SELECT COUNT(DISTINCT sn.common_name) 
                FROM species s 
                JOIN species_names sn ON s.species_name_key = sn.id
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
    conn.close()
    return results

def verify_migration():
    """Verify the database schema and sample data after migration.
    This function checks the column structure of the species and climate tables
    and retrieves sample data to ensure the migration was successful.
    Parameters:
        None
    Returns:
        dict: A dictionary containing:
            - species_columns: List of column names in the species table
            - climate_columns: List of column names in the climate table
            - species_sample: List of 5 sample records from species with joined data
            - climate_sample: List of 5 sample records from climate with joined data
    """
    conn = sqlite3.connect('ecoalert.db')
    cursor = conn.cursor()
    #grok: how do I use pragma? Give me some hints
    cursor.execute("PRAGMA table_info(species)")
    species_columns = cursor.fetchall()
    species_column_names = [col[1] for col in species_columns]
    cursor.execute("PRAGMA table_info(climate)")
    climate_columns = cursor.fetchall()
    climate_column_names = [col[1] for col in climate_columns]
    cursor.execute('''
    SELECT s.id, sn.common_name, ps.status, rlc.red_list_category, s.location_id
    FROM species s
    JOIN species_names sn ON s.species_name_key = sn.id
    JOIN population_status ps ON s.population_status_key = ps.id
    JOIN red_list_cat rlc ON s.red_list_key = rlc.id
    LIMIT 5
    ''')
    species_sample = cursor.fetchall()
    cursor.execute('''
    SELECT c.id, nr.region, c.year, c.temp_anomaly, c.location_id
    FROM climate c
    JOIN noaa_regions nr ON c.geo_key = nr.id
    LIMIT 5
    ''')
    climate_sample = cursor.fetchall()
    conn.close()
    return {
        'species_columns': species_column_names,
        'climate_columns': climate_column_names,
        'species_sample': species_sample,
        'climate_sample': climate_sample
    }
if __name__ == "__main__":
    migrate_data()
    verification = verify_migration()