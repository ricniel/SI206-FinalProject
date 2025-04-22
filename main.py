import sqlite3
import eric_file
import yaya
import json
import matplotlib.pyplot as plt


def join_tables():
    """Merges weather and species tables to show unique species per row.
    Inputs: None
    Outputs: Prints results to console and returns them for visualization."""
    conn = sqlite3.connect('ecoalert.db')
    cursor = conn.cursor()
    cursor.execute('''
        WITH RankedSpecies AS (
            SELECT s.common_name, s.red_list_category, s.location_id,
                   w.date, w.temperature, w.precipitation,
                   ROW_NUMBER() OVER (PARTITION BY s.common_name ORDER BY w.date DESC) as rn
            FROM species s
            JOIN weather w ON s.location_id = w.location_id
            WHERE s.common_name IS NOT NULL AND s.common_name != ''
        )
        SELECT DISTINCT location_id, date, temperature, precipitation, common_name, red_list_category
        FROM RankedSpecies
        WHERE rn = 1
        LIMIT 25
    ''')
    results = cursor.fetchall()
    #Grok: what should I print for the results?
    print("Location ID | Date       | Temp (°C) | Precip (%) | Species Name       | Red List Status")
    print("-" * 80)
    for row in results:
        location_id, date, temp, precip, species_name, red_list = row
        print(f"{location_id:<11} | {date:<10} | {temp:<9.1f} | {precip:<10.1f} | {species_name:<18} | {red_list}")
    conn.close()
    return results

def calculate_stats():
    """Calculates statistics from weather, species, climate, and weather_details tables.
     Parameters: None
    Inputs: None
    Outputs: Prints a table, writes to calculations.txt, and returns results for visualization."""
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
        file.write("Location ID | Avg Temp (°C) | Species Count | Recent Temp Anomaly (°C) | Avg Wind Speed (km/h)\n")
        file.write("-" * 90 + "\n")
        for row in results:
            location_id, avg_temp, species_count, temp_anomaly, avg_wind_speed = row
            temp_anomaly = temp_anomaly if temp_anomaly is not None else 0.0
            avg_wind_speed = avg_wind_speed if avg_wind_speed is not None else 0.0
            #Claude: How do I format the write?
            file.write(f"{location_id:<11} | {avg_temp:<13.1f} | {species_count:<13} | {temp_anomaly:<23.2f} | {avg_wind_speed:<7.1f}\n")

    print("\nLocation ID | Avg Temp (°C) | Species Count | Recent Temp Anomaly (°C) | Avg Wind Speed (km/h)")
    print("-" * 90)
    for row in results:
        location_id, avg_temp, species_count, temp_anomaly, avg_wind_speed = row
        #Grok: How can I ensure that temp_anomaly is include?
        temp_anomaly = temp_anomaly if temp_anomaly is not None else 0.0
        avg_wind_speed = avg_wind_speed if avg_wind_speed is not None else 0.0
        print(f"{location_id:<11} | {avg_temp:<13.1f} | {species_count:<13} | {temp_anomaly:<23.2f} | {avg_wind_speed:<7.1f}")
    conn.close()
    return results

def visualize_data(join_data, stats_data):
    """Creates visualizations from Join Table and Stats Table data.

    Inputs:
    - join_data from join_data function
    - stats_data from calculate stats

    Outputs: Saves visualizations to visualizations.png and displays them."""
    #Grok: Make the location_names variable dict
    location_names = {1: "Antananarivo", 2: "Sao Paulo", 3: "Sydney", 4: "Cape Town", 5: "Buenos Aires",
                      6: "Jakarta", 7: "Mumbai", 8: "Nairobi", 9: "Lima", 10: "Bangkok",
                      11: "Bogotá", 12: "Kuala Lumpur", 13: "Manila", 14: "San José", 15: "London",
                      16: "Hanoi", 17: "Delhi", 18: "Santiago", 19: "Melbourne", 20: "Accra"}

    location_ids = [row[0] for row in stats_data]
    avg_temps = [row[1] for row in stats_data]
    species_counts = [row[2] for row in stats_data]
    location_labels = [location_names[loc_id] for loc_id in location_ids]
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 12))
    ax1.barh(location_labels, avg_temps, color='teal')
    ax1.set_xlabel('Average Temperature (°C)')
    ax1.set_ylabel('Location')
    ax1.set_title('Average Temperature by Location')
    ax1.grid(True, linestyle='--', alpha=0.7, color='gray')
    ax2.barh(location_labels, species_counts, color='coral')
    ax2.set_xlabel('Species Count')
    ax2.set_ylabel('Location')
    ax2.set_title('Species Count by Location')
    ax2.grid(True, linestyle='--', alpha=0.7, color='gray')
    plt.tight_layout()
    plt.savefig("visualizations.png")
    plt.show()

if __name__ == "__main__":
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    try:
        eric_file.create_database()
        yaya.set_up_tables()
        #Grok: How do I ensure this makes 100 rows? instructed me to use for _ throughout the main function. 
        for _ in range(5):
            weather_count = eric_file.store_data()
            print(f"Stored {weather_count} weather rows")
        noaa_regions = ['globe', 'africa', 'europe', 'gulfOfAmerica']
        for _ in range(5):
            noaa_data = yaya.scrape_noaa_data(noaa_regions, driver)
            with open("noaa_data.json", "w") as f:
                json.dump(noaa_data, f)
            noaa_count = yaya.store_noaa_data(noaa_data)
            print(f"Stored {noaa_count} NOAA rows")
        iucn_url = 'https://www.iucnredlist.org/search/list?query=&searchType=species&redListCategory=CR,EN&taxonomies=MAMMALIA,AVES,REPTILIA'
        for _ in range(5):
            iucn_soup = yaya.setup_iucn_webpage_for_scraping(iucn_url, driver)
            iucn_data = yaya.scrape_page_into_dict(iucn_soup)
            with open("iucn_data.json", "w") as f:
                json.dump(iucn_data, f)
            iucn_count = yaya.store_iucn_data(iucn_data)
            print(f"Stored {iucn_count} IUCN rows")
        join_results = join_tables()
        stats_results = calculate_stats()
        visualize_data(join_results, stats_results)
    #Grok: finally is used for quitting the driver. My partner did that section, so I am unfamilar with that code.
    finally:
        driver.quit()