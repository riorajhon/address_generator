import overpy
import requests
import time
import json
import math
from pathlib import Path
import sys
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, BulkWriteError

# -----------------------
# Encoding fix for Windows
# -----------------------
sys.stdout.reconfigure(encoding='utf-8')

# -----------------------
# Config
# -----------------------
# MongoDB Configuration
MONGO_URI = "mongodb://localhost:27017/"  # Change this to your MongoDB URI
DATABASE_NAME = "address_db"
COLLECTION_NAME = "addresses"
FAILED_COLLECTION_NAME = "failed_requests"
SMALL_TYPES_BUILDING = ["house", "hut", "cabin", "studio", "loft", "villa", "apartment"]
SMALL_TYPES_AMENITY = [
    "shop", "kiosk", "booth", "stand", "store",
    "cafe", "bakery", "atm", "pharmacy",
    "bar", "pub", "salon", "clinic", "dentist", "gym"
]

OVERPASS_TIMEOUT = 300
NOMINATIM_DELAY = 1  # seconds per request to respect public API
CACHE_DIR = Path("nominatim_cache")
CACHE_DIR.mkdir(exist_ok=True)

api = overpy.Overpass()

# -----------------------
# Helper functions
# -----------------------
def load_country_city_data():
    with open("country_city_list.json", 'r', encoding='utf-8') as f:
        return json.load(f)

# Global MongoDB collections
client = None
db = None
addresses_collection = None
failed_collection = None

def connect_mongodb():
    """Connect to MongoDB and initialize global collections"""
    global client, db, addresses_collection, failed_collection
    try:
        client = MongoClient(MONGO_URI)
        # Test connection
        client.admin.command('ping')
        db = client[DATABASE_NAME]
        addresses_collection = db[COLLECTION_NAME]
        failed_collection = db[FAILED_COLLECTION_NAME]
        print(f"Connected to MongoDB: {DATABASE_NAME}.{COLLECTION_NAME}")
        return True
    except ConnectionFailure as e:
        print(f"Failed to connect to MongoDB: {e}")
        return False

def insert_address_single(address_data):
    """Insert single address to MongoDB with display_name as unique field"""
    try:
        # Use upsert to avoid duplicates based on display_name (address field)
        result = addresses_collection.update_one(
            {"address": address_data["address"]},  # Filter by display_name
            {"$set": address_data},                # Update with new data
            upsert=True                           # Insert if not exists
        )
        
        if result.upserted_id:
            print(f"✓ Inserted: {address_data['address'][:60]}...")
            return True
        elif result.modified_count > 0:
            print(f"⚠ Updated existing: {address_data['address'][:60]}...")
            return False
        else:
            print(f"→ No change needed: {address_data['address'][:60]}...")
            return False
    except Exception as e:
        # Handle specific MongoDB errors
        if "duplicate key" in str(e).lower() or "11000" in str(e):
            print(f"⚠ Duplicate address (skipped): {address_data['address'][:60]}...")
            return False
        else:
            print(f"✗ Error inserting address: {e}")
            return False

def save_failed_request(osm_type, osm_id, city, country, error_msg):
    """Save failed Nominatim request to separate collection"""
    try:
        prefix = {"node": "N", "way": "W", "relation": "R"}[osm_type]
        osm_param = f"{prefix}{osm_id}"
        
        failed_data = {
            "osm_id": osm_param,
            "osm_type": osm_type,
            "osm_id_number": osm_id,
            "city": city,
            "country": country,
            "error": error_msg,
            "timestamp": time.time()
        }
        
        # Use upsert to avoid duplicates
        result = failed_collection.update_one(
            {"osm_id": osm_param},
            {"$set": failed_data},
            upsert=True
        )
        
        if result.upserted_id:
            print(f"✗ Saved failed request: {osm_param}")
        return True
    except Exception as e:
        print(f"Error saving failed request: {e}")
        return False



def calculate_bbox_area(boundingbox):
    """Calculate bounding box area in square meters"""
    if len(boundingbox) != 4:
        return float('inf')
    
    south, north, west, east = map(float, boundingbox)
    center_lat = (south + north) / 2.0
    lat_m = 111_000
    lon_m = 111_000 * math.cos(math.radians(center_lat))
    height_m = abs(north - south) * lat_m
    width_m = abs(east - west) * lon_m
    return width_m * height_m

def is_home_address(nominatim_result):
    """Check if place_rank >= 20 and bounding box area <= 100m²"""
    if not nominatim_result:
        return False
    for r in nominatim_result:
        place_rank = r.get("place_rank", 0)
        if place_rank >= 20:
            if "boundingbox" in r:
                if calculate_bbox_area(r["boundingbox"]) <= 100:
                    return True
            else:
                return False
    return False

def fetch_nominatim(osm_type, osm_id, city="", country=""):
    """Fetch Nominatim details by OSM type and ID"""
    cache_file = CACHE_DIR / f"{osm_type}{osm_id}.json"
    
    if cache_file.exists():
        try:
            return json.loads(cache_file.read_text(encoding='utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError):
            cache_file.unlink()

    prefix = {"node": "N", "way": "W", "relation": "R"}[osm_type]
    osm_param = f"{prefix}{osm_id}"
    url = "https://nominatim.openstreetmap.org/lookup"
    params = {"osm_ids": osm_param, "format": "json", "addressdetails": 1}
    headers = {"User-Agent": "MIID-Subnet-Miner/1.0", "Accept-Language": "en-US;q=0.9, en;q=0.8"}

    try:
        response = requests.get(url, params=params, headers=headers, timeout=5)
        response.raise_for_status()
        if not response.text.strip():
            # Save failed request
            save_failed_request(osm_type, osm_id, city, country, "Empty response")
            return []
        data = response.json()
        cache_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
        time.sleep(NOMINATIM_DELAY)
        return data
    except Exception as e:
        # Save failed request
        save_failed_request(osm_type, osm_id, city, country, str(e))
        return []

def get_city_wikidata(city_name, country_area_id):
    """Get city Wikidata ID from Overpass - try multiple approaches"""
    city_queries = [
        # Try different admin levels and place types
        f'relation["name"="{city_name}"]["boundary"="administrative"]["admin_level"~"4|5|6|7|8"](area:{country_area_id});',
        f'relation["name:en"="{city_name}"]["boundary"="administrative"]["admin_level"~"4|5|6|7|8"](area:{country_area_id});',
        f'relation["name"="{city_name}"]["place"~"city|town"](area:{country_area_id});',
        f'node["name"="{city_name}"]["place"~"city|town"](area:{country_area_id});',
        f'way["name"="{city_name}"]["place"~"city|town"](area:{country_area_id});'
    ]
    
    for query in city_queries:
        try:
            full_query = f'[out:json][timeout:300];{query}out ids tags;'
            result = api.query(full_query)
            
            # Check relations first
            for rel in result.relations:
                if "wikidata" in rel.tags:
                    print(f"Found city wikidata via relation: {rel.tags['wikidata']}")
                    return rel.tags["wikidata"]
            
            # Then check nodes
            for node in result.nodes:
                if "wikidata" in node.tags:
                    print(f"Found city wikidata via node: {node.tags['wikidata']}")
                    return node.tags["wikidata"]
                    
            # Then check ways
            for way in result.ways:
                if "wikidata" in way.tags:
                    print(f"Found city wikidata via way: {way.tags['wikidata']}")
                    return way.tags["wikidata"]
                    
        except Exception as e:
            print(f"City query failed: {query} - {e}")
            continue
    
    print(f"No wikidata found for {city_name}")
    return None

def fetch_osm_objects(city_name, country_area_id, wikidata_id):
    """Fetch buildings and amenities for a city dynamically"""
    query = f"""
    [out:json][timeout:{OVERPASS_TIMEOUT}];
    area({country_area_id})->.countryArea;
    area["wikidata"="{wikidata_id}"](area.countryArea)->.searchArea;
    (
      node["building"~"{'|'.join(SMALL_TYPES_BUILDING)}"](area.searchArea);
      way["building"~"{'|'.join(SMALL_TYPES_BUILDING)}"](area.searchArea);
      relation["building"~"{'|'.join(SMALL_TYPES_BUILDING)}"](area.searchArea);
      node["amenity"~"{'|'.join(SMALL_TYPES_AMENITY)}"](area.searchArea);
      way["amenity"~"{'|'.join(SMALL_TYPES_AMENITY)}"](area.searchArea);
      relation["amenity"~"{'|'.join(SMALL_TYPES_AMENITY)}"](area.searchArea);
    );
    out center ids;
    """
    return api.query(query)

# -----------------------
# Main
# -----------------------
def main():
    inserted_count = 0
    updated_count = 0
    country_city_data = load_country_city_data()
    
    # Connect to MongoDB
    if not connect_mongodb():
        print("MongoDB connection failed. Exiting.")
        return
    
    # Create unique index on address field (display_name)
    try:
        addresses_collection.create_index("address", unique=True)
        print("Created unique index on 'address' field")
    except Exception as e:
        print(f"Index creation info: {e}")

    for country_code, country_info in country_city_data.items():
        country_name = country_info['country_name']
        cities = country_info['cities']

        # Get country relation ID and area ID - try multiple approaches
        country_queries = [
            f'relation["boundary"="administrative"]["admin_level"="2"]["ISO3166-1"="{country_code}"];',
            f'relation["boundary"="administrative"]["admin_level"="2"]["name"="{country_name}"];',
            f'relation["boundary"="administrative"]["admin_level"="2"]["name:en"="{country_name}"];'
        ]
        
        country_relation_id = None
        for query in country_queries:
            try:
                full_query = f'[out:json];{query}out ids tags;'
                res = api.query(full_query)
                if res.relations:
                    country_relation_id = res.relations[0].id
                    print(f"Found country using query: {query}")
                    break
            except Exception as e:
                print(f"Query failed: {query} - {e}")
                continue
        
        if not country_relation_id:
            print(f"Country {country_name} not found in OSM with any method")
            continue
            
        country_area_id = 3600000000 + country_relation_id
        print(f"Using country area ID: {country_area_id}")

        print(f"\n=== Processing {country_name} ({country_code}) ===")
        print(f"Total cities: {len(cities)}")

        for city in cities:
            print(f"\nFetching city Wikidata for {city}...")
            wikidata_id = get_city_wikidata(city, country_area_id)
            if not wikidata_id:
                print(f"Cannot find Wikidata for {city}, skipping")
                continue

            print(f"Fetching OSM objects for {city}...")
            try:
                osm_data = fetch_osm_objects(city, country_area_id, wikidata_id)
            except Exception as e:
                print(f"Overpass query failed for {city}: {e}")
                continue

            osm_objects = []
            for node in osm_data.nodes:
                osm_objects.append(("node", node.id))
            for way in osm_data.ways:
                osm_objects.append(("way", way.id))
            for rel in osm_data.relations:
                osm_objects.append(("relation", rel.id))

            print(f"Found {len(osm_objects)} OSM objects in {city}")

            for osm_type, osm_id in osm_objects:
                nom_res = fetch_nominatim(osm_type, osm_id, city, country_name)
                if is_home_address(nom_res):
                    for r in nom_res:
                        address_details = r.get("address", {})
                        address_data = {
                            "address": r.get("display_name"),
                            "city": city,
                            "country": address_details.get("country") or country_name,
                            "extra": {
                                "place_id": r.get("place_id"),
                                "osm_type": r.get("osm_type"),
                                "osm_id": r.get("osm_id")
                            },
                            "state": 0
                        }
                        
                        # Insert directly to MongoDB
                        if insert_address_single(address_data):
                            inserted_count += 1
                        else:
                            updated_count += 1

    print(f"\n=== FINAL RESULTS ===")
    print(f"New addresses inserted: {inserted_count}")
    print(f"Existing addresses updated: {updated_count}")
    print(f"Total processed: {inserted_count + updated_count}")
    
    # Close MongoDB connection
    client.close()
    print("MongoDB connection closed")

if __name__ == "__main__":
    main()
