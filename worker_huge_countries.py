#!/usr/bin/env python3
"""
OSM Address Extractor for Huge Countries
Uses chunked processing to handle very large PBF files without std::bad_alloc
Usage: python worker_huge_countries.py <worker_id>
"""

import json
import os
import sys
import signal
import time
import gc
from pathlib import Path
from typing import Optional, Dict, List
from pymongo import MongoClient, ASCENDING
from datetime import datetime
import osmium
from looks_like_address import looks_like_address

# Try to import memory monitoring
try:
    import psutil
    MEMORY_MONITORING = True
except ImportError:
    MEMORY_MONITORING = False

# Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:wkrjk!20020415@localhost:27017/?authSource=admin")
DB_NAME = "address_db"
COUNTRIES_FILE = "geonames_countries.json"
WORK_DIR = Path("./osm_data")
MAX_BBOX_AREA = 100  # m^2

# Chunked processing settings
CHUNK_SIZE = 10  # Process in smaller batches
MEMORY_CHECK_INTERVAL = 1000  # Check memory every N items
MAX_MEMORY_PERCENT = 85  # Stop processing if memory exceeds this

# Global flag for graceful shutdown
shutdown_requested = False

class ChunkedAddressExtractor(osmium.SimpleHandler):
    """Memory-efficient address extractor with chunked processing"""
    
    def __init__(self, worker, country_name: str, country_code: str, max_bbox=100):
        osmium.SimpleHandler.__init__(self)
        self.worker = worker
        self.country_name = country_name
        self.country_code = country_code
        self.max_bbox = max_bbox
        self.processed = 0
        self.found = 0
        self.addresses_batch = []
        self.total_saved = 0
        self.chunk_size = CHUNK_SIZE
        self.memory_checks = 0
        
        # Minimal country mapping for memory efficiency
        self.country_names = {
            'US': 'United States', 'RU': 'Russia', 'CN': 'China', 'CA': 'Canada', 'BR': 'Brazil',
            'AU': 'Australia', 'IN': 'India', 'AR': 'Argentina', 'KZ': 'Kazakhstan', 'DZ': 'Algeria',
            'CD': 'Democratic Republic of the Congo', 'SA': 'Saudi Arabia', 'MX': 'Mexico', 'ID': 'Indonesia',
            'SD': 'Sudan', 'LY': 'Libya', 'IR': 'Iran', 'MN': 'Mongolia', 'PE': 'Peru', 'TD': 'Chad',
            'NE': 'Niger', 'AO': 'Angola', 'EG': 'Egypt', 'TZ': 'Tanzania', 'ZA': 'South Africa',
            'CO': 'Colombia', 'ET': 'Ethiopia', 'BO': 'Bolivia', 'MR': 'Mauritania', 'PK': 'Pakistan',
            'VE': 'Venezuela', 'CL': 'Chile', 'TR': 'Turkey', 'ZM': 'Zambia', 'MM': 'Myanmar',
            'AF': 'Afghanistan', 'SO': 'Somalia', 'CF': 'Central African Republic', 'UA': 'Ukraine',
            'MG': 'Madagascar', 'BW': 'Botswana', 'KE': 'Kenya', 'FR': 'France', 'YE': 'Yemen',
            'TH': 'Thailand', 'ES': 'Spain', 'TM': 'Turkmenistan', 'CM': 'Cameroon', 'PG': 'Papua New Guinea',
            'UZ': 'Uzbekistan', 'IQ': 'Iraq', 'PY': 'Paraguay', 'ZW': 'Zimbabwe', 'JP': 'Japan',
            'DE': 'Germany', 'MY': 'Malaysia', 'VN': 'Vietnam', 'FI': 'Finland', 'IT': 'Italy',
            'PH': 'Philippines', 'BF': 'Burkina Faso', 'NZ': 'New Zealand', 'GA': 'Gabon',
            'GM': 'Gambia', 'GN': 'Guinea', 'GB': 'United Kingdom', 'UG': 'Uganda', 'GH': 'Ghana',
            'RO': 'Romania', 'LA': 'Laos', 'GY': 'Guyana', 'BY': 'Belarus', 'KG': 'Kyrgyzstan',
            'SN': 'Senegal', 'SY': 'Syria', 'KH': 'Cambodia', 'TN': 'Tunisia', 'SL': 'Sierra Leone',
            'BD': 'Bangladesh', 'HN': 'Honduras', 'ER': 'Eritrea', 'JO': 'Jordan', 'GE': 'Georgia',
            'LB': 'Lebanon', 'NI': 'Nicaragua', 'MK': 'Macedonia', 'MW': 'Malawi', 'LR': 'Liberia',
            'BJ': 'Benin', 'CU': 'Cuba', 'GR': 'Greece', 'TG': 'Togo', 'IS': 'Iceland',
            'HU': 'Hungary', 'PT': 'Portugal', 'AZ': 'Azerbaijan', 'AT': 'Austria', 'CZ': 'Czechia',
            'PA': 'Panama', 'SZ': 'Swaziland', 'AE': 'United Arab Emirates', 'JM': 'Jamaica',
            'AM': 'Armenia', 'RW': 'Rwanda', 'TJ': 'Tajikistan', 'AL': 'Albania', 'QA': 'Qatar',
            'NA': 'Namibia', 'LS': 'Lesotho', 'SI': 'Slovenia', 'KW': 'Kuwait', 'FJ': 'Fiji',
            'CY': 'Cyprus', 'TL': 'East Timor', 'BH': 'Bahrain', 'VU': 'Vanuatu', 'ME': 'Montenegro',
            'EE': 'Estonia', 'TT': 'Trinidad and Tobago', 'KM': 'Comoros', 'LU': 'Luxembourg'
        }
    
    def get_country_name(self, country_code):
        """Convert country code to full name"""
        return self.country_names.get(country_code.upper(), self.country_name)
    
    def check_memory_usage(self):
        """Check memory usage and trigger cleanup if needed"""
        if not MEMORY_MONITORING:
            return True
        
        self.memory_checks += 1
        if self.memory_checks % MEMORY_CHECK_INTERVAL != 0:
            return True
        
        mem = psutil.virtual_memory()
        if mem.percent > MAX_MEMORY_PERCENT:
            print(f"[Worker {self.worker.worker_id}] High memory usage: {mem.percent:.1f}%, triggering cleanup")
            
            # Save current batch immediately
            if self.addresses_batch:
                self.worker.save_addresses_batch(self.country_code, self.country_name, self.addresses_batch)
                self.total_saved += len(self.addresses_batch)
                self.addresses_batch.clear()
            
            # Force garbage collection
            gc.collect()
            
            # Check again after cleanup
            mem = psutil.virtual_memory()
            if mem.percent > 90:  # Still too high
                print(f"[Worker {self.worker.worker_id}] Memory still high after cleanup: {mem.percent:.1f}%, pausing processing")
                time.sleep(5)
                return False
        
        return True
    
    def extract_address_info(self, tags):
        """Extract address components from OSM tags (minimal for memory)"""
        address_info = {}
        
        # Only extract essential address components
        for key in ['addr:housenumber', 'addr:street', 'addr:city', 'addr:country', 'name']:
            if key in tags:
                address_info[key.replace('addr:', '')] = tags[key]
        
        if 'building' in tags:
            address_info['building_type'] = tags['building']
            
        return address_info
    
    def format_full_address(self, addr_info, country_name):
        """Format full address string (simplified for memory)"""
        parts = []
        
        # Building name
        if 'name' in addr_info:
            parts.append(addr_info['name'])
        
        # House number and street
        if 'housenumber' in addr_info and 'street' in addr_info:
            parts.append(f"{addr_info['housenumber']} {addr_info['street']}")
        elif 'street' in addr_info:
            parts.append(addr_info['street'])
        
        # City
        if 'city' in addr_info:
            parts.append(addr_info['city'])
        
        # Country
        parts.append(country_name)
        
        return ', '.join(parts) if parts else None
    
    def process_address(self, addr_info):
        """Process address with memory management and 500k limit"""
        # Check if we've reached the 500k limit
        if self.total_saved >= 500000:
            print(f"[Worker {self.worker.worker_id}] Reached 500,000 addresses limit for {self.country_code}, moving to next country")
            self.worker.country_limit_reached = True
            return
        
        # Check memory before processing
        if not self.check_memory_usage():
            return
        
        # Must have at least street or housenumber
        if 'street' not in addr_info and 'housenumber' not in addr_info:
            return
        
        # Get country code and convert to full name
        country_code = addr_info.get('country', self.country_code)
        country_name = self.get_country_name(country_code)
        
        # Format full address
        full_address = self.format_full_address(addr_info, country_name)
        if not full_address or len(full_address) <= 30:
            return
        
        # Don't save if city is unknown
        city = addr_info.get('city', 'Unknown')
        if city == 'Unknown':
            return
        
        # Validate address (skip for performance in huge countries)
        # if not looks_like_address(full_address):
        #     return
        
        # Create address record
        address_record = {
            'street_name': addr_info.get('street', 'Unknown'),
            'city': city,
            'fulladdress': full_address
        }
        
        self.addresses_batch.append(address_record)
        self.found += 1
        
        # Save smaller batches more frequently for memory management
        if len(self.addresses_batch) >= self.chunk_size:
            self.worker.save_addresses_batch(self.country_code, self.country_name, self.addresses_batch)
            self.total_saved += len(self.addresses_batch)
            self.addresses_batch.clear()
            
            # Check if we've reached the limit after saving
            if self.total_saved >= 500000:
                print(f"[Worker {self.worker.worker_id}] Reached 500,000 addresses for {self.country_code}! Moving to next country")
                self.worker.country_limit_reached = True
                return
            
            # Force garbage collection after each chunk
            gc.collect()
            
            if self.total_saved % 1000 == 0:  # Every 100 chunks (10k addresses)
                print(f"[Worker {self.worker.worker_id}] Saved {self.total_saved} addresses for {self.country_code} (Target: 500,000)")
    
    def node(self, n):
        """Process each node with memory management and 500k limit"""
        global shutdown_requested
        
        # Check if country limit reached
        if self.worker.country_limit_reached:
            return
        
        self.processed += 1
        
        # More frequent progress logging for huge countries
        if self.processed % 50000 == 0:
            if MEMORY_MONITORING:
                mem = psutil.virtual_memory()
                print(f"[Worker {self.worker.worker_id}] Processed {self.processed} nodes, found {self.found} addresses, saved {self.total_saved}/500,000, RAM: {mem.percent:.1f}%")
            else:
                print(f"[Worker {self.worker.worker_id}] Processed {self.processed} nodes, found {self.found} addresses, saved {self.total_saved}/500,000")
        
        if shutdown_requested:
            return
        
        # Must be a building with address info
        if 'building' not in n.tags:
            return
        if 'addr:housenumber' not in n.tags and 'addr:street' not in n.tags:
            return
        
        # Extract and process address
        addr_info = self.extract_address_info(n.tags)
        self.process_address(addr_info)
    
    def way(self, w):
        """Process each way with memory management and 500k limit"""
        global shutdown_requested
        
        # Check if country limit reached
        if self.worker.country_limit_reached:
            return
        
        self.processed += 1
        
        # More frequent progress logging for ways
        if self.processed % 5000 == 0:
            if MEMORY_MONITORING:
                mem = psutil.virtual_memory()
                print(f"[Worker {self.worker.worker_id}] Processed {self.processed} ways, found {self.found} addresses, saved {self.total_saved}/500,000, RAM: {mem.percent:.1f}%")
            else:
                print(f"[Worker {self.worker.worker_id}] Processed {self.processed} ways, found {self.found} addresses, saved {self.total_saved}/500,000")
        
        if shutdown_requested:
            return
        
        # Must be a building with address info
        if 'building' not in w.tags:
            return
        if 'addr:housenumber' not in w.tags and 'addr:street' not in w.tags:
            return
        
        # Skip very complex buildings to save memory
        if len(w.nodes) > 500:
            return
        
        # Extract and process address
        addr_info = self.extract_address_info(w.tags)
        self.process_address(addr_info)

class HugeCountryWorker:
    def __init__(self, worker_id: int):
        self.worker_id = worker_id
        self.client = MongoClient(MONGO_URI)
        self.db = self.client[DB_NAME]
        self.addresses_col = self.db.address
        self.country_status_col = self.db.country_status
        self.current_country = None
        self.current_pbf_file = None
        self.country_limit_reached = False  # Flag for 500k limit
        self._init_db()
        WORK_DIR.mkdir(exist_ok=True)
    
    def _init_db(self):
        """Initialize MongoDB collections and indexes"""
        self.addresses_col.create_index([("country", ASCENDING)])
        self.addresses_col.create_index([("status", ASCENDING)])
        self.addresses_col.create_index([("city", ASCENDING)])
        self.addresses_col.create_index([("country", ASCENDING), ("fulladdress", ASCENDING)], unique=True)
        self.country_status_col.create_index([("country_code", ASCENDING)], unique=True)
    
    def claim_country(self) -> Optional[str]:
        """Claim next available country, prioritizing large ones"""
        with open(COUNTRIES_FILE, 'r', encoding='utf-8') as f:
            countries = json.load(f)
        
        # Prioritize large countries that might have been skipped
        large_countries = ['US', 'RU', 'CN', 'CA', 'BR', 'AU', 'IN', 'AR', 'KZ', 'DZ', 'CD', 'SA', 'MX', 'ID']
        
        # First try large countries
        for country_code in large_countries:
            if country_code in countries:
                try:
                    result = self.country_status_col.update_one(
                        {"country_code": country_code},
                        {
                            "$setOnInsert": {
                                "country_code": country_code,
                                "worker_id": self.worker_id,
                                "status": "processing",
                                "started_at": datetime.utcnow()
                            }
                        },
                        upsert=True
                    )
                    
                    if result.upserted_id is not None:
                        return country_code
                    
                    existing = self.country_status_col.find_one({"country_code": country_code})
                    if existing and existing.get("status") in ["completed", "skipped", "processing"]:
                        continue
                        
                except Exception:
                    continue
        
        # Then try all other countries
        for country_code in countries.keys():
            if country_code not in large_countries:
                try:
                    result = self.country_status_col.update_one(
                        {"country_code": country_code},
                        {
                            "$setOnInsert": {
                                "country_code": country_code,
                                "worker_id": self.worker_id,
                                "status": "processing",
                                "started_at": datetime.utcnow()
                            }
                        },
                        upsert=True
                    )
                    
                    if result.upserted_id is not None:
                        return country_code
                    
                    existing = self.country_status_col.find_one({"country_code": country_code})
                    if existing and existing.get("status") in ["completed", "skipped", "processing"]:
                        continue
                        
                except Exception:
                    continue
        
        return None
    
    def find_pbf_file(self, country_code: str) -> Optional[Path]:
        """Find existing PBF file for country"""
        patterns = [
            f"{country_code.lower()}-latest.osm.pbf",
            f"{country_code.upper()}-latest.osm.pbf",
            f"{country_code.lower()}.osm.pbf",
            f"{country_code.upper()}.osm.pbf"
        ]
        
        for pattern in patterns:
            pbf_file = WORK_DIR / pattern
            if pbf_file.exists():
                return pbf_file
        
        return None
    
    def save_addresses_batch(self, country_code: str, country_name: str, addresses: List[Dict]):
        """Save a batch of addresses to MongoDB"""
        if not addresses:
            return
        
        documents = []
        for addr in addresses:
            documents.append({
                "country": country_code,
                "country_name": country_name,
                "street_name": addr['street_name'],
                "city": addr['city'],
                "fulladdress": addr['fulladdress'],
                "status": 0,
                "worker_id": self.worker_id
            })
        
        try:
            result = self.addresses_col.insert_many(documents, ordered=False)
            return len(result.inserted_ids)
        except Exception:
            return len(addresses)
    
    def mark_complete(self, country_code: str):
        """Mark country as completed (fully processed)"""
        self.country_status_col.update_one(
            {"country_code": country_code},
            {"$set": {
                "status": "completed", 
                "completed_at": datetime.utcnow(),
                "completion_type": "full"
            }}
        )
    
    def mark_complete_with_limit(self, country_code: str, addresses_saved: int):
        """Mark country as completed with 500k limit reached"""
        self.country_status_col.update_one(
            {"country_code": country_code},
            {"$set": {
                "status": "completed", 
                "completed_at": datetime.utcnow(),
                "completion_type": "limit_reached",
                "addresses_saved": addresses_saved,
                "limit_reached": True
            }}
        )
        print(f"[Worker {self.worker_id}] Country {country_code} marked as completed with limit (500k addresses)")
    
    def mark_skipped(self, country_code: str, reason: str):
        """Mark country as skipped"""
        self.country_status_col.update_one(
            {"country_code": country_code},
            {"$set": {"status": "skipped", "reason": reason, "skipped_at": datetime.utcnow()}}
        )
    
    def release_country(self, country_code: str):
        """Release country back to pool"""
        self.country_status_col.delete_one({"country_code": country_code})
    
    def process_country(self, country_code: str, country_data: Dict):
        """Process a huge country with chunked processing and 500k limit"""
        global shutdown_requested
        
        country_name = country_data['name']
        self.current_country = country_code
        self.country_limit_reached = False  # Reset limit flag
        
        print(f"[Worker {self.worker_id}] Processing HUGE country: {country_code} - {country_name} (Target: 500,000 addresses)")
        
        try:
            # Find PBF file
            pbf_file = self.find_pbf_file(country_code)
            if not pbf_file:
                print(f"[Worker {self.worker_id}] No PBF file found for {country_code}")
                self.mark_skipped(country_code, "no_pbf_file")
                self.current_country = None
                return
            
            file_size_mb = pbf_file.stat().st_size / (1024 * 1024)
            print(f"[Worker {self.worker_id}] Processing {file_size_mb:.1f}MB PBF file")
            
            self.current_pbf_file = pbf_file
            
            if shutdown_requested:
                return
            
            # Process with chunked handler
            print(f"[Worker {self.worker_id}] Starting chunked extraction from {pbf_file} (will stop at 500k addresses)")
            handler = ChunkedAddressExtractor(self, country_name, country_code, MAX_BBOX_AREA)
            
            try:
                # Force garbage collection before processing
                gc.collect()
                
                # Process the file (will stop automatically at 500k)
                handler.apply_file(str(pbf_file), locations=True)
                
                # Save remaining addresses
                if handler.addresses_batch:
                    self.save_addresses_batch(country_code, country_name, handler.addresses_batch)
                    handler.total_saved += len(handler.addresses_batch)
                
                # Check completion reason
                if self.country_limit_reached:
                    print(f"[Worker {self.worker_id}] COUNTRY LIMIT REACHED! Saved exactly 500,000 addresses for {country_code}")
                    self.mark_complete_with_limit(country_code, handler.total_saved)
                else:
                    print(f"[Worker {self.worker_id}] COUNTRY FULLY PROCESSED! Saved {handler.total_saved} addresses for {country_code}")
                    self.mark_complete(country_code)
                
            except Exception as e:
                error_msg = str(e)
                print(f"[Worker {self.worker_id}] Error processing huge country PBF: {error_msg}")
                
                if "bad_alloc" in error_msg or "memory" in error_msg.lower():
                    print(f"[Worker {self.worker_id}] Memory error in huge country - this shouldn't happen with chunked processing")
                    self.mark_skipped(country_code, "memory_error_chunked")
                else:
                    # If we have some addresses saved, still mark as completed with limit
                    if hasattr(handler, 'total_saved') and handler.total_saved > 0:
                        print(f"[Worker {self.worker_id}] Partial processing completed with {handler.total_saved} addresses")
                        self.mark_complete_with_limit(country_code, handler.total_saved)
                    else:
                        self.release_country(country_code)
                return
            finally:
                if 'handler' in locals():
                    del handler
                gc.collect()
            
            self.current_country = None
            self.current_pbf_file = None
            
        except Exception as e:
            print(f"[Worker {self.worker_id}] Error processing huge country {country_code}: {e}")
            self.release_country(country_code)
    
    def run(self):
        """Main worker loop for huge countries"""
        global shutdown_requested
        
        with open(COUNTRIES_FILE, 'r', encoding='utf-8') as f:
            countries = json.load(f)
        
        while not shutdown_requested:
            country_code = self.claim_country()
            if not country_code:
                print(f"[Worker {self.worker_id}] No more countries to process")
                break
            
            country_data = countries[country_code]
            self.process_country(country_code, country_data)
    
    def cleanup(self):
        """Cleanup resources"""
        print(f"[Worker {self.worker_id}] Cleaning up...")
        
        if self.current_country:
            self.release_country(self.current_country)
        
        self.client.close()
        print(f"[Worker {self.worker_id}] Cleanup complete")

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global shutdown_requested
    print("\n[SIGNAL] Shutdown requested (Ctrl+C). Finishing current operation...")
    shutdown_requested = True

def main():
    global shutdown_requested
    
    if len(sys.argv) != 2:
        print("Usage: python worker_huge_countries.py <worker_id>")
        print("This worker is optimized for huge countries like USA, Russia, China, etc.")
        sys.exit(1)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    worker_id = int(sys.argv[1])
    
    # Print system info
    if MEMORY_MONITORING:
        mem = psutil.virtual_memory()
        print(f"[Worker {worker_id}] Starting HUGE COUNTRY worker with {mem.available / (1024**3):.1f}GB available RAM")
    else:
        print(f"[Worker {worker_id}] Starting HUGE COUNTRY worker (install psutil for memory monitoring)")
    
    worker = HugeCountryWorker(worker_id)
    
    try:
        worker.run()
    except KeyboardInterrupt:
        print(f"\n[Worker {worker_id}] Interrupted by user")
    finally:
        worker.cleanup()
        print(f"[Worker {worker_id}] Shutdown complete")

if __name__ == "__main__":
    main()