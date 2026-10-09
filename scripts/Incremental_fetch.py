import json
import os
import time
from datetime import date, timedelta
import requests

BASE_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
HOURLY_VARS = "carbon_monoxide,carbon_dioxide,pm2_5,pm10,nitrogen_dioxide"

# UPDATED: Pointing directly to your Databricks Staging/Raw Volume!
OUT_DIR = "/Volumes/workspace/default/staging/raw"

# Ensure the directory exists (works natively in Databricks Unity Catalog)
os.makedirs(OUT_DIR, exist_ok=True)

CITIES = {
    # South Asia
    "Lahore": (31.5497, 74.3436), "Delhi": (28.6139, 77.2090),
    "Karachi": (24.8607, 67.0011), "Mumbai": (19.0760, 72.8777),
    "Kolkata": (22.5726, 88.3639), "Bangalore": (12.9716, 77.5946),
    "Hyderabad": (17.3850, 78.4867), "Ahmedabad": (23.0225, 72.5714),
    "Chennai": (13.0827, 80.2707), "Dhaka": (23.8103, 90.4125),
    "Colombo": (6.9271, 79.8612), "Kathmandu": (27.7172, 85.3240),
    "Islamabad": (33.6844, 73.0479), "Kabul": (34.5553, 69.2075),
    # East & Southeast Asia
    "Beijing": (39.9042, 116.4074), "Shanghai": (31.2304, 121.4737),
    "Guangzhou": (23.1291, 113.2644), "Shenzhen": (22.5431, 114.0579),
    "Wuhan": (30.5928, 114.3055), "Chengdu": (30.5728, 104.0668),
    "Xian": (34.3416, 108.9398), "Tianjin": (39.3434, 117.3616),
    "Hong_Kong": (22.3193, 114.1694), "Tokyo": (35.6762, 139.6503),
    "Osaka": (34.6937, 135.5023), "Nagoya": (35.1815, 136.9066),
    "Seoul": (37.5665, 126.9780), "Taipei": (25.0330, 121.5654),
    "Bangkok": (13.7563, 100.5018), "Jakarta": (-6.2088, 106.8456),
    "Manila": (14.5995, 120.9842), "Ho_Chi_Minh_City": (10.8231, 106.6297),
    "Hanoi": (21.0285, 105.8542), "Yangon": (16.8661, 96.1951),
    "Kuala_Lumpur": (3.1390, 101.6869), "Singapore": (1.3521, 103.8198),
    # Middle East
    "Tehran": (35.6892, 51.3890), "Baghdad": (33.3152, 44.3661),
    "Riyadh": (24.7136, 46.6753), "Dubai": (25.2048, 55.2708),
    "Ankara": (39.9334, 32.8597), "Istanbul": (41.0082, 28.9784),
    "Amman": (31.9454, 35.9284), "Beirut": (33.8938, 35.5018),
    # Africa
    "Cairo": (30.0444, 31.2357), "Lagos": (6.5244, 3.3792),
    "Kinshasa": (-4.4419, 15.2663), "Johannesburg": (-26.2041, 28.0473),
    "Cape_Town": (-33.9249, 18.4241), "Nairobi": (-1.2921, 36.8219),
    "Addis_Ababa": (9.0320, 38.7469), "Casablanca": (33.5731, -7.5898),
    "Algiers": (36.7538, 3.0588), "Tunis": (36.8065, 10.1815),
    "Accra": (5.6037, -0.1870), "Dakar": (14.7167, -17.4677),
    "Abidjan": (5.3600, -4.0083), "Kampala": (0.3476, 32.5825),
    "Dar_es_Salaam": (-6.7924, 39.2083), "Luanda": (-8.8390, 13.2894),
    "Harare": (-17.8292, 31.0522),
    # Europe
    "London": (51.5074, -0.1278), "Paris": (48.8566, 2.3522),
    "Berlin": (52.5200, 13.4050), "Madrid": (40.4168, -3.7038),
    "Rome": (41.9028, 12.4964), "Milan": (45.4642, 9.1900),
    "Barcelona": (41.3851, 2.1734), "Lisbon": (38.7223, -9.1393),
    "Amsterdam": (52.3676, 4.9041), "Brussels": (50.8503, 4.3517),
    "Vienna": (48.2082, 16.3738), "Warsaw": (52.2297, 21.0122),
    "Prague": (50.0755, 14.4378), "Budapest": (47.4979, 19.0402),
    "Athens": (37.9838, 23.7275), "Bucharest": (44.4268, 26.1025),
    "Kyiv": (50.4501, 30.5234), "Moscow": (55.7558, 37.6173),
    "Stockholm": (59.3293, 18.0686), "Oslo": (59.9139, 10.7522),
    "Helsinki": (60.1699, 24.9384), "Dublin": (53.3498, -6.2603),
    # North America
    "New_York": (40.7128, -74.0060), "Los_Angeles": (34.0522, -118.2437),
    "Chicago": (41.8781, -87.6298), "Houston": (29.7604, -95.3698),
    "Miami": (25.7617, -80.1918), "Toronto": (43.6532, -79.3832),
    "Vancouver": (49.2827, -123.1207), "Mexico_City": (19.4326, -99.1332),
    # South America
    "Sao_Paulo": (-23.5505, -46.6333), "Rio_de_Janeiro": (-22.9068, -43.1729),
    "Buenos_Aires": (-34.6037, -58.3816), "Lima": (-12.0464, -77.0428),
    "Bogota": (4.7110, -74.0721), "Santiago": (-33.4489, -70.6693),
    # Oceania
    "Sydney": (-33.8688, 151.2093), "Melbourne": (-37.8136, 144.9631),
    "Auckland": (-36.8485, 174.7633),
}

def fetch_incremental_load(start_date: date, end_date: date):
    records = []
    total = len(CITIES)
    
    print(f"--- Starting Incremental Fetch: {start_date} to {end_date} ---")
    
    for i, (city, (lat, lon)) in enumerate(CITIES.items(), start=1):
        print(f"[{i}/{total}] Fetching data for {city}...")
        
        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": HOURLY_VARS,
            "timezone": "auto",
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }
        
        try:
            r = requests.get(BASE_URL, params=params, timeout=60)
            r.raise_for_status()
            
            city_data = r.json()
            city_data["city"] = city
            city_data["source_system"] = "open-meteo-air-quality-api"
            
            records.append(city_data)
        except Exception as e:
            print(f"  FAILED for {city}: {e}")
            
        # RATE LIMITING: Sleep for 2 seconds to prevent HTTP 429 Too Many Requests
        time.sleep(2) 

    # Dynamically name the file based on the dates fetched
    file_name = f"incremental_{start_date.isoformat()}_to_{end_date.isoformat()}.json"
    out_path = f"{OUT_DIR}/{file_name}"
    
    # Writing directly to Unity Catalog Volume
    with open(out_path, "w") as f:
        json.dump(records, f, indent=2)

    total_hours = sum(len(r["hourly"]["time"]) for r in records if "hourly" in r)
    print(f"\n--- Fetch Complete ---")
    print(f"Saved file directly to Data Lake: {out_path}")
    print(f"Successfully processed {len(records)}/{total} cities ({total_hours} hourly readings)")


# ==========================================
# CONFIGURATION: How many days back do you want?
# ==========================================
days_back = 2  # <--- Change this to pull 2 days, 5 days, etc.

# Calculate dates automatically
end_date = date.today() - timedelta(days=1) # Always ends yesterday
start_date = end_date - timedelta(days=(days_back - 1))

# Run the fetch!
fetch_incremental_load(start_date, end_date)

# Optional: Display the contents of your raw folder to prove it saved correctly!
print("\n--- Contents of your /raw/ folder ---")
display(dbutils.fs.ls(OUT_DIR))