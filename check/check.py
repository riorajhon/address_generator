#!/usr/bin/env python3
"""
Address Validation System - Multi-Worker Support
Validates and corrects addresses using Nominatim API
Usage: python check.py <worker_id>
"""

import os
import sys
import time
import requests
from typing import Optional, Dict, List
from pymongo import MongoClient, ASCENDING
from datetime import datetime
import signal
import math
try:
    from .test import validate_address_region
except ImportError:
    from test import validate_address_region

# Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:fjkfjrj!20020415@localhost:27017/?authSource=admin")
DB_NAME = "address_db"
BATCH_SIZE = 1000  # Process addresses in batches for memory safety
NOMINATIM_BASE_URL = "https://nominatim.openstreetmap.org/search"
REQUEST_DELAY = 1.0  # Delay between Nominatim requests (respect rate limits)

# Global variables
shutdown_requested = False
client = None
db = None
addresses_col = None
country_status_col = None
session = None
worker_id = None

def init_db():
    """Initialize MongoDB connections and indexes"""
    global client, db, addresses_col, country_status_col
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    addresses_col = db.address
    country_status_col = db.country_status
    
    # Create indexes
    addresses_col.create_index([("country", ASCENDING)])
    addresses_col.create_index([("city", ASCENDING)])
    country_status_col.create_index([("country_code", ASCENDING)], unique=True)

def init_session():
    """Initialize HTTP session for Nominatim"""
    global session
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    })

def get_addresses_with_fields(country_code: str, skip: int, limit: int) -> List[Dict]:
    """Get addresses for processing"""
    try:
        cursor = addresses_col.find(
            {"country": country_code},
            {
                "_id": 1, 
                "fulladdress": 1,
                "country": 1, 
                "country_name": 1
            }
        ).skip(skip).limit(limit)
        
        return list(cursor)
    except Exception as e:
        print(f"Error getting addresses batch: {e}")
        return []

def query_nominatim(address: str) -> Optional[Dict]:
    """Query Nominatim API for address correction"""
    try:
        params = {
            'q': address,
            'format': 'json',
            'limit': 1,
            'addressdetails': 1,
            'accept-language': 'en'
        }
        
        # Add additional headers for this specific request
        headers = {
            'User-Agent' : 'https://github.com/yanez-compliance/MIID-subnet_1123123'
        }
        
        response = session.get(
            NOMINATIM_BASE_URL, 
            params=params, 
            headers=headers,
            timeout=10
        )
        response.raise_for_status()
        
        data = response.json()
        
        # If API succeeds but returns empty results, mark for deletion
        if not data or len(data) == 0:
            print(f"[Worker {worker_id}] Empty result from Nominatim for '{address}' - marking for deletion")
            return {"delete": True}
        
        result = data[0]
        
        # Check bbox - filter out if bbox > 100m²
        bbox = result.get('boundingbox', [])
        if bbox and len(bbox) >= 4:
            try:
                # bbox format: [min_lat, max_lat, min_lon, max_lon]
                south, north, west, east = map(float, bbox)
                
                # Calculate bbox area in square meters
                center_lat = (south + north) / 2.0
                lat_m = 111_000  # meters per degree latitude
                lon_m = 111_000 * math.cos(math.radians(center_lat))  # meters per degree longitude
                height_m = abs(north - south) * lat_m
                width_m = abs(east - west) * lon_m
                bbox_area_m2 = width_m * height_m
                
                # Filter out if bbox > 100 square meters
                if bbox_area_m2 > 100:
                    print(f"[Worker {worker_id}] Large bbox area: {bbox_area_m2:.1f}m² for '{address}' - marking for deletion")
                    return {"delete": True}
                    
            except (ValueError, TypeError):
                # If bbox parsing fails, continue with the result
                pass
            
            address_details = result.get('address', {})
            
            # Extract detailed address components
            corrected_data = {
                'fulladdress': result.get('display_name', ''),
                'country': address_details.get('country', ''),
                'city': (address_details.get('city') or 
                        address_details.get('town') or 
                        address_details.get('village') or 
                        address_details.get('municipality', '')),
                'street': (address_details.get('road') or 
                          address_details.get('street', ''))
            }
            
            return corrected_data
        
        return None
        
    except Exception as e:
        print(f"[Worker {worker_id}] Nominatim query error for '{address}': {e}")
        return None

def delete_address(address_id) -> bool:
    """Delete address from database"""
    try:
        result = addresses_col.delete_one({"_id": address_id})
        return result.deleted_count > 0
    except Exception as e:
        print(f"[Worker {worker_id}] Error deleting address {address_id}: {e}")
        return False

def update_corrected_address(address_id, corrected_data: Dict) -> bool:
    """Update address with corrected information"""
    try:
        update_fields = {
            "fulladdress": corrected_data.get('fulladdress', ''),
            "country": corrected_data.get('country', ''),
            "city": corrected_data.get('city', ''),
            "street": corrected_data.get('street', ''),
            "worker_id": worker_id
        }
        
        result = addresses_col.update_one(
            {"_id": address_id},
            {"$set": update_fields}
        )
        return result.modified_count > 0
    except Exception as e:
        print(f"[Worker {worker_id}] Error updating address {address_id}: {e}")
        return False

def process_addresses_batch(country_code: str, addresses: List[Dict]) -> Dict:
    """Process a batch of addresses"""
    global shutdown_requested
    
    stats = {
        'processed': 0,
        'valid': 0,
        'corrected': 0,
        'failed': 0,
        'skipped': 0,
        'deleted': 0
    }
    
    for address in addresses:
        if shutdown_requested:
            break
        
        try:
            address_id = address['_id']
            fulladdress = address.get('fulladdress', '')
            country_name = address.get('country_name', country_code)
            
            # Skip if no fulladdress
            if not fulladdress:
                stats['skipped'] += 1
                stats['processed'] += 1
                continue
            
            # Validate address using test.py function
            is_valid = validate_address_region(fulladdress, country_name)
            
            if is_valid:
                # Address is valid, no correction needed
                stats['valid'] += 1
            else:
                # Address is invalid, try to correct it using Nominatim
                print(f"[Worker {worker_id}] Invalid address: {fulladdress}")
                
                # Add delay to respect Nominatim rate limits
                time.sleep(REQUEST_DELAY)
                
                corrected_data = query_nominatim(fulladdress)
                if corrected_data:
                    if corrected_data.get('delete'):
                        # Delete address from database
                        if delete_address(address_id):
                            stats['deleted'] += 1
                            print(f"[Worker {worker_id}] Deleted address: {fulladdress}")
                        else:
                            stats['failed'] += 1
                    elif corrected_data.get('fulladdress'):
                        # Update address with corrected information
                        if validate_address_region(corrected_data.get('fulladdress'), country_name):
                            if update_corrected_address(address_id, corrected_data):
                                stats['corrected'] += 1
                                print(f"[Worker {worker_id}] Corrected: {corrected_data['fulladdress']}")
                            else:
                                stats['failed'] += 1
                        else:
                            delete_address(address_id)
                            stats['deleted'] += 1
                            print(f"[Worker {worker_id}] Deleted address: ------------")
                    else:
                        stats['failed'] += 1
                else:
                    # Could not correct
                    stats['failed'] += 1
            
            stats['processed'] += 1
            
            # Progress update every 100 addresses
            if stats['processed'] % 100 == 0:
                print(f"[Worker {worker_id}] Progress: {stats['processed']} processed, "
                      f"{stats['valid']} valid, {stats['corrected']} corrected, "
                      f"{stats['failed']} failed, {stats['skipped']} skipped, "
                      f"{stats['deleted']} deleted")
            
        except Exception as e:
            print(f"[Worker {worker_id}] Error processing address {address.get('_id')}: {e}")
            stats['failed'] += 1
            stats['processed'] += 1
    
    return stats

def claim_country() -> Optional[str]:
    """Claim next available country for processing"""
    try:
        # First, try to find and claim a country with status "completed"
        result = country_status_col.find_one_and_update(
            {"status": "completed"},
            {
                "$set": {
                    "worker_id": worker_id,
                    "status": "checking",
                    "started_at": datetime.utcnow()
                }
            },
            return_document=True
        )
        
        if result:
            country_code = result["country_code"]
            print(f"[Worker {worker_id}] Claimed completed country: {country_code}")
            return country_code
        
        # If no completed countries, try to create new entries for countries from address collection
        # countries = addresses_col.distinct("country")
        
        # for country_code in countries:
        #     # Try to create new country status entry
        #     try:
        #         country_status_col.insert_one({
        #             "country_code": country_code,
        #             "worker_id": worker_id,
        #             "status": "checking",
        #             "started_at": datetime.utcnow()
        #         })
        #         print(f"[Worker {worker_id}] Claimed new country: {country_code}")
        #         return country_code
        #     except:
        #         # Country already exists, continue to next
        #         continue
        
        return None
        
    except Exception as e:
        print(f"[Worker {worker_id}] Error claiming country: {e}")
        return None

def release_country(country_code: str):
    """Release country back to completed status"""
    try:
        country_status_col.update_one(
            {"country_code": country_code, "worker_id": worker_id},
            {
                "$set": {
                    "status": "checked",
                }
            }
        )
        print(f"[Worker {worker_id}] Released country: {country_code}")
    except Exception as e:
        print(f"[Worker {worker_id}] Error releasing country {country_code}: {e}")

def process_country(country_code: str):
    """Process all addresses for a country"""
    global shutdown_requested
    
    try:
        # Get total address count
        total_addresses = addresses_col.count_documents({"country": country_code})
        if total_addresses == 0:
            print(f"[Worker {worker_id}] No addresses found for {country_code}")
            release_country(country_code)
            return
        
        print(f"[Worker {worker_id}] Country {country_code} has {total_addresses} addresses")
        
        # Process addresses in batches
        skip = 0
        total_stats = {
            'processed': 0,
            'valid': 0,
            'corrected': 0,
            'failed': 0,
            'skipped': 0
        }
        
        while skip < total_addresses and not shutdown_requested:
            print(f"[Worker {worker_id}] Processing batch {skip//BATCH_SIZE + 1}, "
                  f"addresses {skip+1}-{min(skip+BATCH_SIZE, total_addresses)} of {total_addresses}")
            
            # Get batch of addresses
            addresses_batch = get_addresses_with_fields(country_code, skip, BATCH_SIZE)
            if not addresses_batch:
                break
            
            # Process batch
            batch_stats = process_addresses_batch(country_code, addresses_batch)
            
            # Update totals
            for key in total_stats:
                total_stats[key] += batch_stats[key]
            
            skip += BATCH_SIZE
            
            print(f"[Worker {worker_id}] Batch complete. Total progress: {total_stats['processed']}/{total_addresses}")
        
        if not shutdown_requested:
            release_country(country_code)
            print(f"[Worker {worker_id}] Completed {country_code}. Final stats: {total_stats}")
        else:
            # Reset to completed for retry
            release_country(country_code)
            print(f"[Worker {worker_id}] Interrupted {country_code}, released for retry")
        
    except Exception as e:
        print(f"[Worker {worker_id}] Error processing country {country_code}: {e}")
        release_country(country_code)

def run_validation():
    """Main validation function with multi-worker support"""
    global shutdown_requested
    
    print(f"[Worker {worker_id}] Starting address validation system")
    
    while not shutdown_requested:
        # Claim next available country
        country_code = claim_country()
        # country_code = "RU"
        if not country_code:
            print(f"[Worker {worker_id}] No available countries found Finishing work.")
            break
            
        
        # Reset counter when we find work
        print(f"\n[Worker {worker_id}] {'='*50}")
        print(f"[Worker {worker_id}] Processing country: {country_code}")
        print(f"[Worker {worker_id}] {'='*50}")
        
        process_country(country_code)
        # shutdown_requested = True
    
    print(f"[Worker {worker_id}] Validation system stopped")

def cleanup():
    """Cleanup resources"""
    global client, session
    print("Cleaning up...")
    if session:
        session.close()
    if client:
        client.close()
    print("Cleanup complete")

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    global shutdown_requested
    print(f"\n[Worker {worker_id}] Shutdown requested. Finishing current operation...")
    shutdown_requested = True

def main():
    global shutdown_requested, worker_id
    
    # Get worker ID from command line
    if len(sys.argv) != 2:
        print("Usage: python check.py <worker_id>")
        sys.exit(1)
    
    worker_id = int(sys.argv[1])
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Initialize connections
        init_db()
        init_session()
        
        # Run validation
        run_validation()
        
    except KeyboardInterrupt:
        print(f"\n[Worker {worker_id}] Interrupted by user")
    finally:
        cleanup()
        print(f"[Worker {worker_id}] Shutdown complete")

if __name__ == "__main__":
    main()
