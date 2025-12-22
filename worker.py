#!/usr/bin/env python3
"""
Memory-Optimized OSM Address Extractor Worker
Handles large OSM files with limited memory using streaming and chunking
Usage: python worker_memory_optimized.py <worker_id>
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
from geofabrik_urls import get_geofabrik_url, GEOFABRIK_URLS

# Optional memory monitoring
try:
    import psutil
    MEMORY_MONITORING = True
except ImportError:
    MEMORY_MONITORING = False
    print("Warning: psutil not installed. Memory monitoring disabled.")

# Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:wkrjk!20020415@localhost:27017/?authSource=admin")
DB_NAME = "address_db"
COUNTRIES_FILE = "geonames_countries.json"
WORK_DIR = Path("./osm_data")
MAX_BBOX_AREA = 100  # m^2
MAX_ADDRESSES_PER_COUNTRY = 300000  # Address limit per country
BATCH_SIZE = 50  # Smaller batch size for memory efficiency
MEMORY_CHECK_INTERVAL = 1000  # Check memory every N processed items

# Global flag for graceful shutdown
shutdown_requested = False

class MemoryOptimizedAddressExtractor(osmium.SimpleHandler):
    """Memory-optimized address extractor with streaming and chunking"""
    
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
        self.limit_reached = False
        self.memory_warning_shown = False
        
        # Memory management
        self.last_memory_check = 0
        self.memory_threshold = 0.85  # Stop if memory usage > 85%
        
        # Minimal country mapping (only what we need)
        self.country_names = {
            'AD': 'Andorra', 'AE': 'United Arab Emirates', 'AF': 'Afghanistan',
            'AL': 'Albania', 'AM': 'Armenia', 'AO': 'Angola', 'AQ': 'Antarctica',
            'AR': 'Argentina', 'AT': 'Austria', 'AU': 'Australia', 'AZ': 'Azerbaijan',
            'BA': 'Bosnia and Herzegovina', 'BD': 'Bangladesh', 'BE': 'Belgium',
            'BF': 'Burkina Faso', 'BG': 'Bulgaria', 'BH': 'Bahrain', 'BI': 'Burundi',
            'BJ': 'Benin', 'BN': 'Brunei', 'BO': 'Bolivia', 'BR': 'Brazil',
            'BS': 'Bahamas', 'BT': 'Bhutan', 'BW': 'Botswana', 'BY': 'Belarus',
            'BZ': 'Belize', 'CA': 'Canada', 'CD': 'Democratic Republic of the Congo',
            'CF': 'Central African Republic', 'CG': 'Republic of the Congo',
            'CH': 'Switzerland', 'CI': 'Ivory Coast', 'CL': 'Chile', 'CM': 'Cameroon',
            'CN': 'China', 'CO': 'Colombia', 'CR': 'Costa Rica', 'CU': 'Cuba',
            'CV': 'Cabo Verde', 'CY': 'Cyprus', 'CZ': 'Czechia', 'DE': 'Germany',
            'DJ': 'Djibouti', 'DK': 'Denmark', 'DO': 'Dominican Republic',
            'DZ': 'Algeria', 'EC': 'Ecuador', 'EE': 'Estonia', 'EG': 'Egypt',
            'ER': 'Eritrea', 'ES': 'Spain', 'ET': 'Ethiopia', 'FI': 'Finland',
            'FJ': 'Fiji', 'FR': 'France', 'GA': 'Gabon', 'GB': 'United Kingdom',
            'GE': 'Georgia', 'GH': 'Ghana', 'GN': 'Guinea', 'GQ': 'Equatorial Guinea',
            'GR': 'Greece', 'GT': 'Guatemala', 'GW': 'Guinea-Bissau', 'GY': 'Guyana',
            'HN': 'Honduras', 'HR': 'Croatia', 'HT': 'Haiti', 'HU': 'Hungary',
            'ID': 'Indonesia', 'IE': 'Ireland', 'IL': 'Israel', 'IN': 'India',
            'IQ': 'Iraq', 'IR': 'Iran', 'IS': 'Iceland', 'IT': 'Italy',
            'JM': 'Jamaica', 'JO': 'Jordan', 'JP': 'Japan', 'KE': 'Kenya',
            'KG': 'Kyrgyzstan', 'KH': 'Cambodia', 'KP': 'North Korea',
            'KR': 'South Korea', 'KW': 'Kuwait', 'KZ': 'Kazakhstan', 'LA': 'Laos',
            'LB': 'Lebanon', 'LI': 'Liechtenstein', 'LK': 'Sri Lanka', 'LR': 'Liberia',
            'LS': 'Lesotho', 'LT': 'Lithuania', 'LU': 'Luxembourg', 'LV': 'Latvia',
            'LY': 'Libya', 'MA': 'Morocco', 'MC': 'Monaco', 'MD': 'Moldova',
            'ME': 'Montenegro', 'MG': 'Madagascar', 'MK': 'Macedonia', 'ML': 'Mali',
            'MM': 'Myanmar', 'MN': 'Mongolia', 'MR': 'Mauritania', 'MT': 'Malta',
            'MU': 'Mauritius', 'MW': 'Malawi', 'MX': 'Mexico', 'MY': 'Malaysia',
            'MZ': 'Mozambique', 'NA': 'Namibia', 'NE': 'Niger', 'NG': 'Nigeria',
            'NI': 'Nicaragua', 'NL': 'Netherlands', 'NO': 'Norway', 'NP': 'Nepal',
            'NZ': 'New Zealand', 'OM': 'Oman', 'PA': 'Panama', 'PE': 'Peru',
            'PG': 'Papua New Guinea', 'PH': 'Philippines', 'PK': 'Pakistan',
            'PL': 'Poland', 'PT': 'Portugal', 'PY': 'Paraguay', 'QA': 'Qatar',
            'RO': 'Romania', 'RS': 'Serbia', 'RU': 'Russia', 'RW': 'Rwanda',
            'SA': 'Saudi Arabia', 'SD': 'Sudan', 'SE': 'Sweden', 'SG': 'Singapore',
            'SI': 'Slovenia', 'SK': 'Slovakia', 'SL': 'Sierra Leone', 'SN': 'Senegal',
            'SO': 'Somalia', 'SR': 'Suriname', 'SS': 'South Sudan', 'SY': 'Syria',
            'TD': 'Chad', 'TG': 'Togo', 'TH': 'Thailand', 'TJ': 'Tajikistan',
            'TN': 'Tunisia', 'TR': 'Turkey', 'TT': 'Trinidad and Tobago',
            'TZ': 'Tanzania', 'UA': 'Ukraine', 'UG': 'Uganda', 'US': 'United States',
            'UY': 'Uruguay', 'UZ': 'Uzbekistan', 'VE': 'Venezuela', 'VN': 'Vietnam',
            'YE': 'Yemen', 'ZA': 'South Africa', 'ZM': 'Zambia', 'ZW': 'Zimbabwe'
        }
    
    def check_memory_usage(self):
        """Check memory usage and return True if we should continue"""
        if not MEMORY_MONITORING:
            return True
            
        try:
            memory = psutil.virtual_memory()
            if memory.percent > self.memory_threshold * 100:
                if not self.memory_warning_shown:
                    print(f"[Worker {self.worker.worker_id}] WARNING: Memory usage high ({memory.percent:.1f}%)")
                    self.memory_warning_shown = True
                
                # Force garbage collection
                gc.collect()
                
                # Check again after GC
                memory = psutil.virtual_memory()
                if memory.percent > 90:  # Critical threshold
                    print(f"[Worker {self.worker.worker_id}] CRITICAL: Memory usage {memory.percent:.1f}% - stopping processing")
                    return False
            return True
        except:
            return True
    
    def get_country_name(self, country_code):
        """Convert country code to full name"""
        return self.country_names.get(country_code.upper(), self.country_name)
    
    def calculate_bbox(self, nodes):
        """Calculate bounding box size (optimized for memory)"""
        if len(nodes) < 2:
            return 0
        
        # Process nodes in chunks to avoid memory issues
        valid_coords = []
        for n in nodes:
            if n.location.valid():
                valid_coords.append((n.lat, n.lon))
                if len(valid_coords) > 100:  # Limit to avoid memory issues
                    break
        
        if len(valid_coords) < 2:
            return 0
        
        lats = [coord[0] for coord in valid_coords]
        lons = [coord[1] for coord in valid_coords]
        
        lat_diff = max(lats) - min(lats)
        lon_diff = max(lons) - min(lons)
        
        # Convert to meters (approximate)
        lat_meters = lat_diff * 111000
        lon_meters = lon_diff * 111000 * 0.7
        
        return max(lat_meters, lon_meters)
    
    def extract_address_info(self, tags):
        """Extract address components (memory optimized)"""
        # Only extract what we need to minimize memory usage
        addr_info = {}
        
        # Essential address components only
        for key in ['addr:housenumber', 'addr:street', 'addr:city', 'addr:country']:
            if key in tags:
                addr_info[key.replace('addr:', '')] = tags[key]
        
        # Optional components
        for key in ['addr:suburb', 'addr:postcode', 'building', 'name']:
            if key in tags:
                if key.startswith('addr:'):
                    addr_info[key.replace('addr:', '')] = tags[key]
                else:
                    addr_info[key] = tags[key]
        
        return addr_info
    
    def format_full_address(self, addr_info, country_name):
        """Format address string (memory optimized)"""
        parts = []
        
        # Building name
        if 'name' in addr_info:
            parts.append(addr_info['name'])
        
        # House number and street
        if 'housenumber' in addr_info and 'street' in addr_info:
            parts.append(f"{addr_info['housenumber']} {addr_info['street']}")
        elif 'street' in addr_info:
            parts.append(addr_info['street'])
        
        # Suburb
        if 'suburb' in addr_info:
            parts.append(addr_info['suburb'])
        
        # City
        if 'city' in addr_info:
            parts.append(addr_info['city'])
        
        # Postcode
        if 'postcode' in addr_info:
            parts.append(addr_info['postcode'])
        
        # Country
        parts.append(country_name)
        
        return ', '.join(parts) if parts else None
    
    def process_address(self, addr_info):
        """Process address with memory optimization"""
        # Quick validation
        # if not ('street' in addr_info or 'housenumber' in addr_info):
        #     return False
        if not ('street' in addr_info):
            return False
        # Get country
        country_code = addr_info.get('country', self.country_code)
        country_name = self.get_country_name(country_code)
        
        # Format address
        full_address = self.format_full_address(addr_info, country_name)
        if not full_address or len(full_address) <= 30:
            return False
        
        # Check city
        city = addr_info.get('city', 'Unknown')
        if city == 'Unknown':
            return False
        
        # Validate address
        if not looks_like_address(full_address):
            return False
        
        # Create minimal record
        address_record = {
            'street_name': addr_info.get('street', 'Unknown'),
            'city': city,
            'fulladdress': full_address
        }
        
        self.addresses_batch.append(address_record)
        self.found += 1
        
        # Save smaller batches more frequently
        if len(self.addresses_batch) >= BATCH_SIZE:
            self.worker.save_addresses_batch(self.country_code, self.country_name, self.addresses_batch)
            self.total_saved += len(self.addresses_batch)
            self.addresses_batch.clear()
            
            # Force garbage collection after each batch
            gc.collect()
            
            print(f"[Worker {self.worker.worker_id}] Saved batch, total: {self.total_saved} addresses for {self.country_code}")
            
            # Check address limit
            if self.total_saved >= MAX_ADDRESSES_PER_COUNTRY:
                print(f"[Worker {self.worker.worker_id}] Reached limit of {MAX_ADDRESSES_PER_COUNTRY} addresses for {self.country_code}")
                self.limit_reached = True
                return False
        
        return True
    
    # def node(self, n):
    #     """Process node with memory management"""
    #     global shutdown_requested
        
    #     self.processed += 1
        
    #     # Memory check every N items
    #     if self.processed % MEMORY_CHECK_INTERVAL == 0:
    #         if not self.check_memory_usage():
    #             print(f"[Worker {self.worker.worker_id}] Stopping due to memory constraints")
    #             self.limit_reached = True
    #             return
            
    #         print(f"[Worker {self.worker.worker_id}] Processed {self.processed} nodes, found {self.found} addresses")
        
    #     if shutdown_requested or self.limit_reached:
    #         return
        
    #     # Must be a building with address
    #     if 'building' not in n.tags:
    #         return
    #     if 'addr:housenumber' not in n.tags and 'addr:street' not in n.tags:
    #         return
        
    #     # Extract and process
    #     addr_info = self.extract_address_info(n.tags)
    #     self.process_address(addr_info)
    
    def way(self, w):
        """Process way with memory management"""
        global shutdown_requested
        
        self.processed += 1
        
        # Memory check every N items
        if self.processed % (MEMORY_CHECK_INTERVAL // 10) == 0:  # Check more frequently for ways
            if not self.check_memory_usage():
                print(f"[Worker {self.worker.worker_id}] Stopping due to memory constraints")
                self.limit_reached = True
                return
            
            print(f"[Worker {self.worker.worker_id}] Processed {self.processed} ways, found {self.found} addresses")
        
        if shutdown_requested or self.limit_reached:
            return
        
        # Must be a building with address
        if 'building' not in w.tags:
            return
        # if 'addr:housenumber' not in w.tags and 'addr:street' not in w.tags:
        #     return
        if 'addr:street' not in w.tags:
            return
        # Check bounding box (memory efficient)
        try:
            bbox = self.calculate_bbox(w.nodes)
            if bbox > self.max_bbox:
                return
        except:
            return
        
        # Extract and process
        addr_info = self.extract_address_info(w.tags)
        self.process_address(addr_info)

class MemoryOptimizedWorker:
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
    
    def claim_country(self) -> Optional[str]:
        """Claim next available country"""
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
                    print(f"[Worker {self.worker_id}] Claimed country: {country_code}")
                    return country_code
                
                existing = self.country_status_col.find_one({"country_code": country_code})
                if existing:
                    status = existing.get("status")
                    if status == "retry":
                        # Country marked for retry - claim it
                        return country_code
                    elif status in ["completed", "skipped", "processing"]:
                        # Already processed, skipped, or being processed by another worker
                        continue
                
            except Exception:
                continue
        
        return None
    
    def check_file_size(self, pbf_file: Path) -> bool:
        """Check if file is manageable with available memory"""
        try:
            file_size = pbf_file.stat().st_size
            
            if MEMORY_MONITORING:
                available_memory = psutil.virtual_memory().available
                # More conservative estimate for large files
                estimated_memory_needed = file_size * 2  # Reduced from 4x
                
                if estimated_memory_needed > available_memory:
                    size_mb = file_size / (1024 * 1024)
                    mem_mb = available_memory / (1024 * 1024)
                    print(f"[Worker {self.worker_id}] File too large: {size_mb:.1f}MB, available memory: {mem_mb:.1f}MB")
                    return False
            else:
                # Without psutil, use conservative file size limits
                if file_size > 200 * 1024 * 1024:  # 200MB limit without memory monitoring
                    print(f"[Worker {self.worker_id}] File too large: {file_size / (1024 * 1024):.1f}MB (no memory monitoring)")
                    return False
            
            return True
        except Exception:
            return True
    
    def download_pbf(self, country_code: str, country_name: str) -> Optional[Path]:
        """Download PBF file (same as enhanced version)"""
        pbf_file = WORK_DIR / f"{country_code.lower()}-latest.osm.pbf"
        
        if pbf_file.exists():
            print(f"[Worker {self.worker_id}] PBF already exists: {pbf_file}")
            return pbf_file
        
        if country_code.upper() not in GEOFABRIK_URLS:
            print(f"[Worker {self.worker_id}] No Geofabrik URL found for {country_code}")
            return None
        
        geofabrik_url = get_geofabrik_url(country_code, country_name)
        
        try:
            print(f"[Worker {self.worker_id}] Downloading from Geofabrik: {geofabrik_url}")
            urllib.request.urlretrieve(geofabrik_url, pbf_file)
            
            if pbf_file.exists() and pbf_file.stat().st_size > 1000:
                print(f"[Worker {self.worker_id}] Downloaded: {pbf_file} ({pbf_file.stat().st_size} bytes)")
                return pbf_file
            
        except Exception as e:
            print(f"[Worker {self.worker_id}] Download failed: {e}")
            if pbf_file.exists():
                pbf_file.unlink()
        
        return None
    
    def save_addresses_batch(self, country_code: str, country_name: str, addresses: List[Dict]):
        """Save addresses with memory optimization"""
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
        """Release country for retry"""
        self.country_status_col.delete_one({"country_code": country_code})
    
    def process_country(self, country_code: str, country_data: Dict):
        """Process country with memory optimization"""
        global shutdown_requested
        
        country_name = country_data['name']
        self.current_country = country_code
        print(f"[Worker {self.worker_id}] Processing {country_code} - {country_name}")
        
        try:
            # Download PBF
            pbf_file = self.download_pbf(country_code, country_name)
            if not pbf_file:
                if country_code.upper() not in GEOFABRIK_URLS:
                    self.mark_skipped(country_code, "no_geofabrik_url")
                else:
                    self.mark_skipped(country_code, "download_failed")
                self.current_country = None
                return
            
            # Check file size
            if not self.check_file_size(pbf_file):
                self.mark_skipped(country_code, "file_too_large")
                self.current_country = None
                return
            
            self.current_pbf_file = pbf_file
            
            # Process with memory optimization
            print(f"[Worker {self.worker_id}] Processing {pbf_file} (memory optimized)")
            
            # Force garbage collection before processing
            gc.collect()
            
            handler = MemoryOptimizedAddressExtractor(self, country_name, country_code)
            
            try:
                handler.apply_file(str(pbf_file), locations=True)
                
                # Save final batch
                if handler.addresses_batch and not handler.limit_reached:
                    remaining_capacity = MAX_ADDRESSES_PER_COUNTRY - handler.total_saved
                    if remaining_capacity > 0:
                        addresses_to_save = handler.addresses_batch[:remaining_capacity]
                        if addresses_to_save:
                            self.save_addresses_batch(country_code, country_name, addresses_to_save)
                            handler.total_saved += len(addresses_to_save)
                
                if handler.limit_reached:
                    print(f"[Worker {self.worker_id}] Completed {country_code} with limit: {handler.total_saved} addresses")
                else:
                    print(f"[Worker {self.worker_id}] Completed {country_code}: {handler.total_saved} addresses")
                
            except Exception as e:
                error_msg = str(e)
                print(f"[Worker {self.worker_id}] Processing error: {error_msg}")
                
                if "memory" in error_msg.lower() or "bad_alloc" in error_msg:
                    self.mark_skipped(country_code, "memory_error")
                else:
                    self.release_country(country_code)
                return
            finally:
                # Aggressive cleanup
                if 'handler' in locals():
                    del handler
                gc.collect()
            
            # Mark complete
            self.mark_complete(country_code)
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
        gc.collect()
        print(f"[Worker {self.worker_id}] Cleanup complete")

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    global shutdown_requested
    print("\n[SIGNAL] Shutdown requested. Finishing current operation...")
    shutdown_requested = True

def main():
    global shutdown_requested
    
    if len(sys.argv) != 2:
        print("Usage: python worker_memory_optimized.py <worker_id>")
        sys.exit(1)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    worker_id = int(sys.argv[1])
    worker = MemoryOptimizedWorker(worker_id)
    
    try:
        worker.run()
    except KeyboardInterrupt:
        print(f"\n[Worker {worker_id}] Interrupted by user")
    finally:
        worker.cleanup()
        print(f"[Worker {worker_id}] Shutdown complete")

if __name__ == "__main__":
    main()