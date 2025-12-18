#!/usr/bin/env python3
"""
Memory-Safe OSM Address Extractor Worker
Handles large PBF files by checking memory limits and skipping oversized files
Usage: python worker_memory_safe.py <worker_id>
"""

import json
import os
import sys
import signal
import time
import gc
from pathlib import Path
from typing import Optional, Dict, List
import urllib.request
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
    print("Warning: psutil not installed. Install with: pip install psutil")

# Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:wkrjk!20020415@localhost:27017/?authSource=admin")
DB_NAME = "address_db"
COUNTRIES_FILE = "geonames_countries.json"
WORK_DIR = Path("./osm_data")
MAX_BBOX_AREA = 100  # m^2

# Memory limits (in MB)
MAX_FILE_SIZE_MB = 500  # Skip files larger than 500MB if no memory monitoring
MEMORY_SAFETY_FACTOR = 4  # PBF processing needs ~4x file size in RAM

# Global flag for graceful shutdown
shutdown_requested = False

class MemorySafeExtractor(osmium.SimpleHandler):
    """Memory-safe address extractor with batch processing"""
    
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
        self.batch_size = 50  # Smaller batches for memory safety
        
        # Country code to full name mapping (abbreviated for memory)
        self.country_names = {
            'US': 'United States', 'GB': 'United Kingdom', 'DE': 'Germany', 'FR': 'France',
            'IT': 'Italy', 'ES': 'Spain', 'PL': 'Poland', 'RO': 'Romania', 'NL': 'Netherlands',
            'BE': 'Belgium', 'GR': 'Greece', 'PT': 'Portugal', 'CZ': 'Czechia', 'HU': 'Hungary',
            'SE': 'Sweden', 'AT': 'Austria', 'BY': 'Belarus', 'CH': 'Switzerland', 'BG': 'Bulgaria',
            'RS': 'Serbia', 'DK': 'Denmark', 'FI': 'Finland', 'SK': 'Slovakia', 'NO': 'Norway',
            'IE': 'Ireland', 'HR': 'Croatia', 'BA': 'Bosnia and Herzegovina', 'AL': 'Albania',
            'LT': 'Lithuania', 'SI': 'Slovenia', 'LV': 'Latvia', 'EE': 'Estonia', 'MK': 'Macedonia',
            'MD': 'Moldova', 'LU': 'Luxembourg', 'MT': 'Malta', 'IS': 'Iceland'
        }
    
    def get_country_name(self, country_code):
        """Convert country code to full name"""
        return self.country_names.get(country_code.upper(), self.country_name)
    
    def extract_address_info(self, tags):
        """Extract address components from OSM tags"""
        address_info = {}
        
        # Address components
        if 'addr:housenumber' in tags:
            address_info['housenumber'] = tags['addr:housenumber']
        if 'addr:street' in tags:
            address_info['street'] = tags['addr:street']
        if 'addr:city' in tags:
            address_info['city'] = tags['addr:city']
        if 'addr:suburb' in tags:
            address_info['suburb'] = tags['addr:suburb']
        if 'addr:postcode' in tags:
            address_info['postcode'] = tags['addr:postcode']
        if 'addr:country' in tags:
            address_info['country'] = tags['addr:country']
        
        # Building info
        if 'building' in tags:
            address_info['building_type'] = tags['building']
        if 'name' in tags:
            address_info['building_name'] = tags['name']
            
        return address_info
    
    def format_full_address(self, addr_info, country_name):
        """Format full address string"""
        parts = []
        
        # Building name (if available)
        if 'building_name' in addr_info:
            parts.append(addr_info['building_name'])
        
        # House number and street
        if 'housenumber' in addr_info and 'street' in addr_info:
            parts.append(f"{addr_info['housenumber']} {addr_info['street']}")
        elif 'street' in addr_info:
            parts.append(addr_info['street'])
        
        # Suburb (if available)
        if 'suburb' in addr_info:
            parts.append(addr_info['suburb'])
        
        # City
        if 'city' in addr_info:
            parts.append(addr_info['city'])
        
        # Postcode
        if 'postcode' in addr_info:
            parts.append(addr_info['postcode'])
        
        # Country (use full name)
        parts.append(country_name)
        
        return ', '.join(parts) if parts else None
    
    def process_address(self, addr_info):
        """Common address processing logic"""
        # Must have at least street or housenumber
        if 'street' not in addr_info and 'housenumber' not in addr_info:
            return
        
        # Get country code and convert to full name
        country_code = addr_info.get('country', self.country_code)
        country_name = self.get_country_name(country_code)
        
        # Format full address
        full_address = self.format_full_address(addr_info, country_name)
        if not full_address:
            return
        
        # Filter: full address must be longer than 30 characters
        if len(full_address) <= 30:
            return
        
        # Don't save if city is unknown
        city = addr_info.get('city', 'Unknown')
        if city == 'Unknown':
            return
        
        # Validate address using looks_like_address function
        if not looks_like_address(full_address):
            return
        
        # Create address record
        address_record = {
            'street_name': addr_info.get('street', 'Unknown'),
            'city': city,
            'fulladdress': full_address
        }
        
        self.addresses_batch.append(address_record)
        self.found += 1
        
        # Save batch when it reaches batch_size (smaller for memory safety)
        if len(self.addresses_batch) >= self.batch_size:
            self.worker.save_addresses_batch(self.country_code, self.country_name, self.addresses_batch)
            self.total_saved += len(self.addresses_batch)
            self.addresses_batch.clear()
            
            # Force garbage collection after each batch
            if self.total_saved % 500 == 0:  # Every 10 batches
                gc.collect()
            
            print(f"[Worker {self.worker.worker_id}] Saved batch, total: {self.total_saved} addresses for {self.country_code}")
    
    def node(self, n):
        """Process each node (point with address)"""
        global shutdown_requested
        
        self.processed += 1
        
        # Progress logging with memory check
        if self.processed % 25000 == 0:  # More frequent logging
            if MEMORY_MONITORING:
                mem = psutil.virtual_memory()
                print(f"[Worker {self.worker.worker_id}] Processed {self.processed} nodes, found {self.found} addresses, RAM: {mem.percent:.1f}%")
            else:
                print(f"[Worker {self.worker.worker_id}] Processed {self.processed} nodes, found {self.found} addresses")
        
        if shutdown_requested:
            return
        
        # Must be a building
        if 'building' not in n.tags:
            return
        
        # Must have address information
        if 'addr:housenumber' not in n.tags and 'addr:street' not in n.tags:
            return
        
        # Extract address info and process
        addr_info = self.extract_address_info(n.tags)
        self.process_address(addr_info)
    
    def way(self, w):
        """Process each way (building)"""
        global shutdown_requested
        
        self.processed += 1
        
        # Progress logging with memory check
        if self.processed % 2500 == 0:  # More frequent logging for ways
            if MEMORY_MONITORING:
                mem = psutil.virtual_memory()
                print(f"[Worker {self.worker.worker_id}] Processed {self.processed} ways, found {self.found} addresses, RAM: {mem.percent:.1f}%")
            else:
                print(f"[Worker {self.worker.worker_id}] Processed {self.processed} ways, found {self.found} addresses")
        
        if shutdown_requested:
            return
        
        # Must be a building
        if 'building' not in w.tags:
            return
        
        # Must have address information
        if 'addr:housenumber' not in w.tags and 'addr:street' not in w.tags:
            return
        
        # Calculate bounding box (simplified for memory)
        try:
            if len(w.nodes) > 2:
                # Simple bbox check without storing all coordinates
                bbox_check = len(w.nodes) < 1000  # Skip very complex buildings
                if not bbox_check:
                    return
        except:
            return
        
        # Extract address info and process
        addr_info = self.extract_address_info(w.tags)
        self.process_address(addr_info)

class MemorySafeWorker:
    def __init__(self, worker_id: int):
        self.worker_id = worker_id
        self.client = MongoClient(MONGO_URI)
        self.db = self.client[DB_NAME]
        self.addresses_col = self.db.address
        self.country_status_col = self.db.country_status
        self.current_country = None
        self.current_pbf_file = None
        self._init_db()
        WORK_DIR.mkdir(exist_ok=True)
    
    def _init_db(self):
        """Initialize MongoDB collections and indexes"""
        self.addresses_col.create_index([("country", ASCENDING)])
        self.addresses_col.create_index([("status", ASCENDING)])
        self.addresses_col.create_index([("city", ASCENDING)])
        self.addresses_col.create_index([("country", ASCENDING), ("fulladdress", ASCENDING)], unique=True)
        self.country_status_col.create_index([("country_code", ASCENDING)], unique=True)
    
    def check_memory_safety(self, pbf_file: Path) -> tuple[bool, str]:
        """Check if PBF file can be safely processed"""
        file_size = pbf_file.stat().st_size
        file_size_mb = file_size / (1024 * 1024)
        
        if MEMORY_MONITORING:
            mem = psutil.virtual_memory()
            available_mb = mem.available / (1024 * 1024)
            estimated_needed_mb = file_size_mb * MEMORY_SAFETY_FACTOR
            
            if estimated_needed_mb > available_mb:
                return False, f"File: {file_size_mb:.1f}MB, needs ~{estimated_needed_mb:.1f}MB, available: {available_mb:.1f}MB"
            
            if mem.percent > 80:  # System already using >80% RAM
                return False, f"System memory usage already high: {mem.percent:.1f}%"
            
            return True, f"File: {file_size_mb:.1f}MB, estimated need: {estimated_needed_mb:.1f}MB, available: {available_mb:.1f}MB"
        else:
            # Without memory monitoring, use conservative file size limits
            if file_size_mb > MAX_FILE_SIZE_MB:
                return False, f"File too large: {file_size_mb:.1f}MB > {MAX_FILE_SIZE_MB}MB (no memory monitoring)"
            
            return True, f"File size OK: {file_size_mb:.1f}MB"
    
    def claim_country(self) -> Optional[str]:
        """Claim next available country for processing"""
        with open(COUNTRIES_FILE, 'r', encoding='utf-8') as f:
            countries = json.load(f)
        
        for country_code in countries.keys():
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
        # Try different naming patterns
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
        """Mark country as completed"""
        self.country_status_col.update_one(
            {"country_code": country_code},
            {"$set": {"status": "completed", "completed_at": datetime.utcnow()}}
        )
    
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
        """Process a single country with memory safety"""
        global shutdown_requested
        
        country_name = country_data['name']
        self.current_country = country_code
        print(f"[Worker {self.worker_id}] Processing {country_code} - {country_name}")
        
        try:
            # Find PBF file
            pbf_file = self.find_pbf_file(country_code)
            if not pbf_file:
                print(f"[Worker {self.worker_id}] No PBF file found for {country_code}")
                self.mark_skipped(country_code, "no_pbf_file")
                self.current_country = None
                return
            
            # Check memory safety
            safe, reason = self.check_memory_safety(pbf_file)
            if not safe:
                print(f"[Worker {self.worker_id}] Skipping {country_code} - {reason}")
                self.mark_skipped(country_code, "memory_unsafe")
                self.current_country = None
                return
            
            print(f"[Worker {self.worker_id}] Memory check passed - {reason}")
            self.current_pbf_file = pbf_file
            
            if shutdown_requested:
                return
            
            # Process with memory-safe handler
            print(f"[Worker {self.worker_id}] Extracting addresses from {pbf_file}")
            handler = MemorySafeExtractor(self, country_name, country_code, MAX_BBOX_AREA)
            
            try:
                # Force garbage collection before processing
                gc.collect()
                
                handler.apply_file(str(pbf_file), locations=True)
                
                # Save remaining addresses
                if handler.addresses_batch:
                    self.save_addresses_batch(country_code, country_name, handler.addresses_batch)
                    handler.total_saved += len(handler.addresses_batch)
                
                print(f"[Worker {self.worker_id}] Completed! Saved {handler.total_saved} addresses for {country_code}")
                self.mark_complete(country_code)
                
            except Exception as e:
                error_msg = str(e)
                print(f"[Worker {self.worker_id}] Error processing PBF: {error_msg}")
                
                if "bad_alloc" in error_msg or "memory" in error_msg.lower():
                    print(f"[Worker {self.worker_id}] Memory error despite safety checks")
                    self.mark_skipped(country_code, "memory_error")
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
            print(f"[Worker {self.worker_id}] Error processing {country_code}: {e}")
            self.release_country(country_code)
    
    def run(self):
        """Main worker loop"""
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
        print("Usage: python worker_memory_safe.py <worker_id>")
        print("Note: Install psutil for better memory monitoring: pip install psutil")
        sys.exit(1)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    worker_id = int(sys.argv[1])
    
    # Print memory info at startup
    if MEMORY_MONITORING:
        mem = psutil.virtual_memory()
        print(f"[Worker {worker_id}] Starting with {mem.available / (1024**3):.1f}GB available RAM ({mem.percent:.1f}% used)")
    else:
        print(f"[Worker {worker_id}] Starting (no memory monitoring - install psutil for better safety)")
    
    worker = MemorySafeWorker(worker_id)
    
    try:
        worker.run()
    except KeyboardInterrupt:
        print(f"\n[Worker {worker_id}] Interrupted by user")
    finally:
        worker.cleanup()
        print(f"[Worker {worker_id}] Shutdown complete")

if __name__ == "__main__":
    main()