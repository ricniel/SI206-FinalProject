import sqlite3
import eric_file
import csv
import seaborn as sns
import numpy as np
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

def get_species_count_by_region_and_status():
    """
    Gets region, red list name, and species count grouped by region and red list category from iucn database

    Parameters
    ----------------------------
    none

    Returns
    ----------------------------
    csv: "species_temp_data.csv"
        csv with columns "Region", "Red List Name", "Species Count"
    """
    conn = sqlite3.connect('iucn.db')
    cur = conn.cursor()
    
    cur.execute("""
        SELECT noaa_regions.region, red_list_cat.red_list_category, COUNT(*) as species_count
        FROM species
        JOIN noaa_regions ON noaa_regions.id = species.geo_key
        JOIN red_list_cat ON red_list_cat.id = species.red_list_key 
        WHERE species.red_list_key >= ?
        GROUP BY noaa_regions.id, species.red_list_key
    """, (2,))
    
    data = cur.fetchall()

    with open('species_temp_data.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Region", "Red List Name", "Species Count"])
        writer.writerows(data)

def heatmap():
    """
    Creates heat map from species_temp_data.csv

    Parameters
    ----------------------------
    none

    Returns
    ----------------------------
    png: heatmap_viz.png
    
    WRITTEN WITH HELP FROM CHATGPT
    """
    # Step 1: Define custom Red List category order
    custom_order = [
        "Least Concern",
        "Near Threatened",
        "Vulnerable",
        "Endangered",
        "Critically Endangered",
        "Extinct in the Wild",
        "Extinct"
    ]

    # Step 2: Read data from CSV
    species_file = 'species_temp_data.csv'
    species_data = []
    regions_set = set()
    valid_names = set(custom_order)

    with open(species_file, newline='') as file:
        reader = csv.DictReader(file)
        for row in reader:
            region = row['Region']
            name = row['Red List Name']
            if name not in valid_names:
                continue  # skip unknown categories
            count = int(row['Species Count'])
            species_data.append((region, name, count))
            regions_set.add(region)

    # Step 3: Build matrix 
    regions = sorted(regions_set)
    region_idx = {r: i for i, r in enumerate(regions)}
    name_idx = {n: i for i, n in enumerate(custom_order)}

    matrix = np.zeros((len(regions), len(custom_order)), dtype=int)

    for region, name, count in species_data:
        i = region_idx[region]
        j = name_idx[name]
        matrix[i][j] = count

    # Step 4: Plot heatmap
    plt.figure(figsize=(12, 8))
    sns.heatmap(
        matrix,
        annot=True,
        fmt="d",
        cmap="YlGnBu",
        xticklabels=custom_order,
        yticklabels=regions,
        linewidths=0.5,
        cbar_kws={'label': 'Species Count'}
    )

    plt.title("Species Count by Region and Red List Category")
    plt.xlabel("Red List Category")
    plt.ylabel("Region")
    plt.tight_layout()
    plt.savefig('heatmap_viz.png')
    plt.show()

if __name__ == "__main__":
    eric_file.create_database()
    #yaya.set_up_tables()
    #Grok: How do I ensure this makes 100 rows? instructed me to use for _ throughout the main function. 
    for _ in range(5):
        weather_count = eric_file.store_data()
        print(f"Stored {weather_count} weather rows")
    noaa_regions = ['globe', 'africa', 'europe', 'gulfOfAmerica']

    join_results = join_tables()
    stats_results = calculate_stats()
    visualize_data(join_results, stats_results)
    get_species_count_by_region_and_status()
    heatmap()