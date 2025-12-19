#!/usr/bin/env python3
"""
Enhanced OSM Address Extractor Worker
- Downloads OSM files using geofabrik_urls.py
- Saves missing countries to JSON file for manual download
- Processes countries from geonames_countries.json in parallel across 50 workers
Usage: python worker_enhanced.py <worker_id>
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
MISSING_COUNTRIES_FILE = "missing_countries.json"
MAX_BBOX_AREA = 100  # m^2

# Global flag for graceful shutdown
shutdown_requested = False

# Note: Missing countries are now tracked in MongoDB status collection

class AddressExtractor(osmium.SimpleHandler):
    """Extract buildings with full addresses from OSM PBF"""
    
    def __init__(self, worker, country_name: str, country_code: str, max_bbox=100, max_addresses=300000):
        osmium.SimpleHandler.__init__(self)
        self.worker = worker
        self.country_name = country_name
        self.country_code = country_code
        self.max_bbox = max_bbox
        self.max_addresses = max_addresses  # Limit per country
        self.processed = 0
        self.found = 0
        self.addresses_batch = []
        self.total_saved = 0
        self.limit_reached = False  # Flag to stop processing
        
        # Country code to full name mapping
        self.country_names = {
            'AD': 'Andorra', 'AE': 'United Arab Emirates', 'AF': 'Afghanistan',
            'AG': 'Antigua and Barbuda', 'AI': 'Anguilla', 'AL': 'Albania',
            'AM': 'Armenia', 'AO': 'Angola', 'AQ': 'Antarctica', 'AR': 'Argentina',
            'AS': 'American Samoa', 'AT': 'Austria', 'AU': 'Australia', 'AW': 'Aruba',
            'AX': 'Aland Islands', 'AZ': 'Azerbaijan', 'BA': 'Bosnia and Herzegovina',
            'BB': 'Barbados', 'BD': 'Bangladesh', 'BE': 'Belgium', 'BF': 'Burkina Faso',
            'BG': 'Bulgaria', 'BH': 'Bahrain', 'BI': 'Burundi', 'BJ': 'Benin',
            'BL': 'Saint Barthelemy', 'BM': 'Bermuda', 'BN': 'Brunei', 'BO': 'Bolivia',
            'BQ': 'Bonaire, Saint Eustatius and Saba', 'BR': 'Brazil', 'BS': 'Bahamas',
            'BT': 'Bhutan', 'BV': 'Bouvet Island', 'BW': 'Botswana', 'BY': 'Belarus',
            'BZ': 'Belize', 'CA': 'Canada', 'CC': 'Cocos Islands', 'CD': 'Democratic Republic of the Congo',
            'CF': 'Central African Republic', 'CG': 'Republic of the Congo', 'CH': 'Switzerland',
            'CI': 'Ivory Coast', 'CK': 'Cook Islands', 'CL': 'Chile', 'CM': 'Cameroon',
            'CN': 'China', 'CO': 'Colombia', 'CR': 'Costa Rica', 'CU': 'Cuba',
            'CV': 'Cabo Verde', 'CW': 'Curacao', 'CX': 'Christmas Island', 'CY': 'Cyprus',
            'CZ': 'Czechia', 'DE': 'Germany', 'DJ': 'Djibouti', 'DK': 'Denmark',
            'DM': 'Dominica', 'DO': 'Dominican Republic', 'DZ': 'Algeria', 'EC': 'Ecuador',
            'EE': 'Estonia', 'EG': 'Egypt', 'EH': 'Western Sahara', 'ER': 'Eritrea',
            'ES': 'Spain', 'ET': 'Ethiopia', 'FI': 'Finland', 'FJ': 'Fiji',
            'FK': 'Falkland Islands', 'FM': 'Micronesia', 'FO': 'Faroe Islands', 'FR': 'France',
            'GA': 'Gabon', 'GB': 'United Kingdom', 'GD': 'Grenada', 'GE': 'Georgia',
            'GF': 'French Guiana', 'GG': 'Guernsey', 'GH': 'Ghana', 'GI': 'Gibraltar',
            'GL': 'Greenland', 'GM': 'Gambia', 'GN': 'Guinea', 'GP': 'Guadeloupe',
            'GQ': 'Equatorial Guinea', 'GR': 'Greece', 'GS': 'South Georgia and the South Sandwich Islands',
            'GT': 'Guatemala', 'GU': 'Guam', 'GW': 'Guinea-Bissau', 'GY': 'Guyana',
            'HK': 'Hong Kong', 'HM': 'Heard Island and McDonald Islands', 'HN': 'Honduras',
            'HR': 'Croatia', 'HT': 'Haiti', 'HU': 'Hungary', 'ID': 'Indonesia',
            'IE': 'Ireland', 'IL': 'Israel', 'IM': 'Isle of Man', 'IN': 'India',
            'IO': 'British Indian Ocean Territory', 'IQ': 'Iraq', 'IR': 'Iran', 'IS': 'Iceland',
            'IT': 'Italy', 'JE': 'Jersey', 'JM': 'Jamaica', 'JO': 'Jordan',
            'JP': 'Japan', 'KE': 'Kenya', 'KG': 'Kyrgyzstan', 'KH': 'Cambodia',
            'KI': 'Kiribati', 'KM': 'Comoros', 'KN': 'Saint Kitts and Nevis', 'KP': 'North Korea',
            'KR': 'South Korea', 'KW': 'Kuwait', 'KY': 'Cayman Islands', 'KZ': 'Kazakhstan',
            'LA': 'Laos', 'LB': 'Lebanon', 'LC': 'Saint Lucia', 'LI': 'Liechtenstein',
            'LK': 'Sri Lanka', 'LR': 'Liberia', 'LS': 'Lesotho', 'LT': 'Lithuania',
            'LU': 'Luxembourg', 'LV': 'Latvia', 'LY': 'Libya', 'MA': 'Morocco',
            'MC': 'Monaco', 'MD': 'Moldova', 'ME': 'Montenegro', 'MF': 'Saint Martin',
            'MG': 'Madagascar', 'MH': 'Marshall Islands', 'MK': 'Macedonia', 'ML': 'Mali',
            'MM': 'Myanmar', 'MN': 'Mongolia', 'MO': 'Macao', 'MP': 'Northern Mariana Islands',
            'MQ': 'Martinique', 'MR': 'Mauritania', 'MS': 'Montserrat', 'MT': 'Malta',
            'MU': 'Mauritius', 'MV': 'Maldives', 'MW': 'Malawi', 'MX': 'Mexico',
            'MY': 'Malaysia', 'MZ': 'Mozambique', 'NA': 'Namibia', 'NC': 'New Caledonia',
            'NE': 'Niger', 'NF': 'Norfolk Island', 'NG': 'Nigeria', 'NI': 'Nicaragua',
            'NL': 'Netherlands', 'NO': 'Norway', 'NP': 'Nepal', 'NR': 'Nauru',
            'NU': 'Niue', 'NZ': 'New Zealand', 'OM': 'Oman', 'PA': 'Panama',
            'PE': 'Peru', 'PF': 'French Polynesia', 'PG': 'Papua New Guinea', 'PH': 'Philippines',
            'PK': 'Pakistan', 'PL': 'Poland', 'PM': 'Saint Pierre and Miquelon', 'PN': 'Pitcairn',
            'PR': 'Puerto Rico', 'PS': 'Palestine', 'PT': 'Portugal', 'PW': 'Palau',
            'PY': 'Paraguay', 'QA': 'Qatar', 'RE': 'Reunion', 'RO': 'Romania',
            'RS': 'Serbia', 'RU': 'Russia', 'RW': 'Rwanda', 'SA': 'Saudi Arabia',
            'SB': 'Solomon Islands', 'SC': 'Seychelles', 'SD': 'Sudan', 'SE': 'Sweden',
            'SG': 'Singapore', 'SH': 'Saint Helena', 'SI': 'Slovenia', 'SJ': 'Svalbard and Jan Mayen',
            'SK': 'Slovakia', 'SL': 'Sierra Leone', 'SM': 'San Marino', 'SN': 'Senegal',
            'SO': 'Somalia', 'SR': 'Suriname', 'SS': 'South Sudan', 'ST': 'Sao Tome and Principe',
            'SV': 'El Salvador', 'SX': 'Sint Maarten', 'SY': 'Syria', 'SZ': 'Swaziland',
            'TC': 'Turks and Caicos Islands', 'TD': 'Chad', 'TF': 'French Southern Territories',
            'TG': 'Togo', 'TH': 'Thailand', 'TJ': 'Tajikistan', 'TK': 'Tokelau',
            'TL': 'East Timor', 'TM': 'Turkmenistan', 'TN': 'Tunisia', 'TO': 'Tonga',
            'TR': 'Turkey', 'TT': 'Trinidad and Tobago', 'TV': 'Tuvalu', 'TW': 'Taiwan',
            'TZ': 'Tanzania', 'UA': 'Ukraine', 'UG': 'Uganda', 'UM': 'United States Minor Outlying Islands',
            'US': 'United States', 'UY': 'Uruguay', 'UZ': 'Uzbekistan', 'VA': 'Vatican City',
            'VC': 'Saint Vincent and the Grenadines', 'VE': 'Venezuela', 'VG': 'British Virgin Islands',
            'VI': 'U.S. Virgin Islands', 'VN': 'Vietnam', 'VU': 'Vanuatu', 'WF': 'Wallis and Futuna',
            'WS': 'Samoa', 'YE': 'Yemen', 'YT': 'Mayotte', 'ZA': 'South Africa',
            'ZM': 'Zambia', 'ZW': 'Zimbabwe'
        }
    
    def get_country_name(self, country_code):
        """Convert country code to full name"""
        return self.country_names.get(country_code.upper(), self.country_name)
    
    def calculate_bbox(self, nodes):
        """Calculate bounding box size (max distance between nodes)"""
        if len(nodes) < 2:
            return 0
            
        lats = [n.lat for n in nodes if n.location.valid()]
        lons = [n.lon for n in nodes if n.location.valid()]
        
        if not lats or not lons:
            return 0
            
        lat_diff = max(lats) - min(lats)
        lon_diff = max(lons) - min(lons)
        
        # Convert to meters (approximate)
        lat_meters = lat_diff * 111000  # 1 degree lat ≈ 111km
        lon_meters = lon_diff * 111000 * 0.7  # Adjust for latitude
        
        bbox = max(lat_meters, lon_meters)
        return bbox
    
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
        """Format full address string with full country name, building, and suburb"""
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
    
    def node(self, n):
        """Process each node (point with address)"""
        global shutdown_requested
        
        self.processed += 1
        
        # Progress logging
        if self.processed % 10000 == 0:
            print(f"[Worker {self.worker.worker_id}] Processed {self.processed} nodes, found {self.found} addresses")
        
        if shutdown_requested or self.limit_reached:
            return
        
        # Must be a building
        if 'building' not in n.tags:
            return
        
        # Must have address information
        if 'addr:housenumber' not in n.tags and 'addr:street' not in n.tags:
            return
        
        # Extract address info
        addr_info = self.extract_address_info(n.tags)
        
        # Must have at least street or housenumber
        if 'street' not in addr_info and 'housenumber' not in addr_info:
            return
        
        # Get country code and convert to full name
        country_code = addr_info.get('country', self.country_code)
        country_name = self.get_country_name(country_code)
        
        # Format full address with full country name
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
        
        # Save batch when it reaches 100
        if len(self.addresses_batch) >= 100:
            self.worker.save_addresses_batch(self.country_code, self.country_name, self.addresses_batch)
            self.total_saved += len(self.addresses_batch)
            self.addresses_batch.clear()
            print(f"[Worker {self.worker.worker_id}] Saved batch, total: {self.total_saved} addresses for {self.country_code}")
            
            # Check if we've reached the limit
            if self.total_saved >= self.max_addresses:
                print(f"[Worker {self.worker.worker_id}] Reached limit of {self.max_addresses} addresses for {self.country_code}")
                self.limit_reached = True
    
    def way(self, w):
        """Process each way (building)"""
        global shutdown_requested
        
        self.processed += 1
        
        # Progress logging
        if self.processed % 1000 == 0:
            print(f"[Worker {self.worker.worker_id}] Processed {self.processed} ways, found {self.found} addresses")
        
        if shutdown_requested or self.limit_reached:
            return
        
        # Must be a building
        if 'building' not in w.tags:
            return
        
        # Must have address information
        if 'addr:housenumber' not in w.tags and 'addr:street' not in w.tags:
            return
        
        # Calculate bounding box
        try:
            bbox = self.calculate_bbox(w.nodes)
            if bbox > self.max_bbox:
                return  # Skip if bbox too large
        except:
            return
        
        # Extract address info
        addr_info = self.extract_address_info(w.tags)
        
        # Must have at least street or housenumber
        if 'street' not in addr_info and 'housenumber' not in addr_info:
            return
        
        # Get country code and convert to full name
        country_code = addr_info.get('country', self.country_code)
        country_name = self.get_country_name(country_code)
        
        # Format full address with full country name
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
        
        # Save batch when it reaches 100
        if len(self.addresses_batch) >= 100:
            self.worker.save_addresses_batch(self.country_code, self.country_name, self.addresses_batch)
            self.total_saved += len(self.addresses_batch)
            self.addresses_batch.clear()
            print(f"[Worker {self.worker.worker_id}] Saved batch, total: {self.total_saved} addresses for {self.country_code}")
            
            # Check if we've reached the limit
            if self.total_saved >= self.max_addresses:
                print(f"[Worker {self.worker.worker_id}] Reached limit of {self.max_addresses} addresses for {self.country_code}")
                self.limit_reached = True

class EnhancedAddressWorker:
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
        # Create indexes for addresses collection
        self.addresses_col.create_index([("country", ASCENDING)])
        self.addresses_col.create_index([("status", ASCENDING)])
        self.addresses_col.create_index([("city", ASCENDING)])
        self.addresses_col.create_index([("country", ASCENDING), ("fulladdress", ASCENDING)], unique=True)
        
        # Create index for country_status collection
        self.country_status_col.create_index([("country_code", ASCENDING)], unique=True)
    
    def claim_country(self) -> Optional[str]:
        """Claim next available country for processing - atomic operation to prevent race conditions"""
        with open(COUNTRIES_FILE, 'r', encoding='utf-8') as f:
            countries = json.load(f)
        
        for country_code in countries.keys():
            try:
                # First, try to insert a new document (for unclaimed countries)
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
                
                # If we inserted a new document, we successfully claimed it
                if result.upserted_id is not None:
                    print(f"[Worker {self.worker_id}] Claimed country: {country_code}")
                    return country_code
                
                # If document already exists, check if it's available
                existing = self.country_status_col.find_one({"country_code": country_code})
                if existing:
                    status = existing.get("status")
                    if status == "retry":
                        # Country marked for retry - claim it
                        return country_code
                    elif status in ["completed", "skipped", "processing"]:
                        # Already processed, skipped, or being processed by another worker
                        continue
            except Exception as e:
                # Duplicate key or other error, try next country
                continue
        
        return None
    
    def safe_remove_file(self, file_path: Path, max_retries: int = 3) -> bool:
        """Safely remove a file with retry logic for Windows file locking"""
        # Force garbage collection to release any file handles
        gc.collect()
        
        for attempt in range(max_retries):
            try:
                if file_path.exists():
                    # Try renaming first (often works when delete doesn't)
                    temp_name = file_path.with_suffix(f'.tmp_{int(time.time())}_{attempt}')
                    file_path.rename(temp_name)
                    temp_name.unlink()
                return True
            except (PermissionError, OSError) as e:
                if attempt < max_retries - 1:
                    print(f"[Worker {self.worker_id}] File locked, retrying in {attempt + 1} seconds...")
                    time.sleep(attempt + 1)
                    gc.collect()  # Force garbage collection again
                else:
                    print(f"[Worker {self.worker_id}] Failed to remove {file_path} after {max_retries} attempts: {e}")
                    # As last resort, try to rename to .corrupted
                    try:
                        corrupted_name = file_path.with_suffix('.corrupted')
                        file_path.rename(corrupted_name)
                        print(f"[Worker {self.worker_id}] Renamed corrupted file to {corrupted_name}")
                        return True
                    except:
                        return False
            except Exception as e:
                print(f"[Worker {self.worker_id}] Error removing {file_path}: {e}")
                return False
        return False

    def validate_pbf_file(self, pbf_file: Path) -> bool:
        """Validate PBF file by trying to read it with osmium"""
        handler = None
        try:
            # Try to create a simple handler and read the file
            class ValidationHandler(osmium.SimpleHandler):
                def __init__(self):
                    osmium.SimpleHandler.__init__(self)
                    self.valid = True
                    self.count = 0
                
                def node(self, n):
                    self.count += 1
                    if self.count > 10:  # Just read first 10 nodes to validate
                        raise StopIteration()
            
            handler = ValidationHandler()
            try:
                handler.apply_file(str(pbf_file))
            except StopIteration:
                pass  # Expected when we stop after 10 nodes
            
            return True
        except Exception as e:
            print(f"[Worker {self.worker_id}] PBF validation failed: {e}")
            return False
        finally:
            # Ensure handler is cleaned up
            if handler:
                del handler
            gc.collect()  # Force garbage collection to release file handles
    
    def download_pbf(self, country_code: str, country_name: str) -> Optional[Path]:
        """Download OSM PBF file for country using ONLY geofabrik_urls.py"""
        pbf_file = WORK_DIR / f"{country_code.lower()}-latest.osm.pbf"
        
        # Check if existing file is valid
        if pbf_file.exists():
            print(f"[Worker {self.worker_id}] PBF already exists: {pbf_file}")
            if self.validate_pbf_file(pbf_file):
                return pbf_file
            else:
                print(f"[Worker {self.worker_id}] Existing PBF file is corrupted, re-downloading...")
                self.safe_remove_file(pbf_file)
        
        # Check if URL exists in geofabrik_urls.py - ONLY source of truth
        if country_code.upper() not in GEOFABRIK_URLS:
            print(f"[Worker {self.worker_id}] No Geofabrik URL found for {country_code} ({country_name})")
            return None  # Return None to trigger skipped status
        
        # Get the ONLY URL from geofabrik_urls.py
        geofabrik_url = get_geofabrik_url(country_code, country_name)
        
        try:
            print(f"[Worker {self.worker_id}] Downloading from Geofabrik: {geofabrik_url}")
            urllib.request.urlretrieve(geofabrik_url, pbf_file)
            
            # Validate PBF file size and format
            if pbf_file.exists() and pbf_file.stat().st_size > 1000:  # At least 1KB
                # Validate PBF file with osmium
                if self.validate_pbf_file(pbf_file):
                    print(f"[Worker {self.worker_id}] Downloaded: {pbf_file} ({pbf_file.stat().st_size} bytes)")
                    return pbf_file
            
            # If validation fails, delete file
            print(f"[Worker {self.worker_id}] Downloaded file appears corrupted")
            self.safe_remove_file(pbf_file)
            
        except Exception as e:
            print(f"[Worker {self.worker_id}] Failed to download from {geofabrik_url}: {e}")
            self.safe_remove_file(pbf_file)
        
        # If download failed
        print(f"[Worker {self.worker_id}] Download failed for {country_code} ({country_name})")
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
        
        # Bulk insert with ordered=False to continue on duplicate key errors
        try:
            result = self.addresses_col.insert_many(documents, ordered=False)
            return len(result.inserted_ids)
        except Exception as e:
            # Some duplicates are expected, return batch size as estimate
            return len(addresses)
    
    def mark_complete(self, country_code: str):
        """Mark country as completed"""
        self.country_status_col.update_one(
            {"country_code": country_code},
            {
                "$set": {
                    "status": "completed",
                    "completed_at": datetime.utcnow()
                }
            }
        )
    
    def release_country(self, country_code: str):
        """Release country back to pool (for interrupted processing)"""
        print(f"[Worker {self.worker_id}] Releasing country {country_code} back to pool")
        self.country_status_col.delete_one({"country_code": country_code})
    
    def cleanup(self):
        """Cleanup resources on shutdown"""
        print(f"[Worker {self.worker_id}] Cleaning up...")
        
        # Release current country if processing
        if self.current_country:
            self.release_country(self.current_country)
        
        # Keep PBF files (don't delete them)
        if self.current_pbf_file and self.current_pbf_file.exists():
            print(f"[Worker {self.worker_id}] Keeping PBF file: {self.current_pbf_file}")
        
        # Close MongoDB connection
        self.client.close()
        print(f"[Worker {self.worker_id}] Cleanup complete")
    
    def mark_skipped(self, country_code: str, reason: str):
        """Mark country as skipped (no download available)"""
        self.country_status_col.update_one(
            {"country_code": country_code},
            {
                "$set": {
                    "status": "skipped",
                    "reason": reason,
                    "skipped_at": datetime.utcnow()
                }
            }
        )
    
    def check_file_size(self, pbf_file: Path) -> bool:
        """Check if PBF file is too large for available memory"""
        try:
            import psutil
            file_size = pbf_file.stat().st_size
            available_memory = psutil.virtual_memory().available
            
            # Rule of thumb: PBF processing needs 3-5x file size in RAM
            estimated_memory_needed = file_size * 4
            
            if estimated_memory_needed > available_memory:
                size_mb = file_size / (1024 * 1024)
                mem_mb = available_memory / (1024 * 1024)
                print(f"[Worker {self.worker_id}] File too large: {size_mb:.1f}MB, available memory: {mem_mb:.1f}MB")
                return False
            return True
        except ImportError:
            # If psutil not available, check basic file size limits
            file_size = pbf_file.stat().st_size
            # Skip files larger than 500MB if we can't check memory
            if file_size > 500 * 1024 * 1024:
                print(f"[Worker {self.worker_id}] File too large: {file_size / (1024 * 1024):.1f}MB (no memory check available)")
                return False
            return True

    def process_country(self, country_code: str, country_data: Dict):
        """Process a single country with memory management"""
        global shutdown_requested
        
        country_name = country_data['name']
        self.current_country = country_code
        print(f"[Worker {self.worker_id}] Processing {country_code} - {country_name}")
        
        try:
            # Step 1: Download PBF
            if shutdown_requested:
                return
            
            pbf_file = self.download_pbf(country_code, country_name)
            if not pbf_file:
                # Check if it's because no Geofabrik URL exists
                if country_code.upper() not in GEOFABRIK_URLS:
                    print(f"[Worker {self.worker_id}] Skipping {country_code} - no Geofabrik URL available")
                    self.mark_skipped(country_code, "no_geofabrik_url")
                else:
                    print(f"[Worker {self.worker_id}] Skipping {country_code} - download failed")
                    self.mark_skipped(country_code, "download_failed")
                self.current_country = None
                return
            
            # Step 1.5: Check if file is too large for available memory
            if not self.check_file_size(pbf_file):
                print(f"[Worker {self.worker_id}] Skipping {country_code} - file too large for available memory")
                self.mark_skipped(country_code, "file_too_large")
                self.current_country = None
                return
            
            self.current_pbf_file = pbf_file
            
            # Step 2: Extract addresses using osmium
            if shutdown_requested:
                return
            
            print(f"[Worker {self.worker_id}] Extracting addresses from {pbf_file}")
            handler = AddressExtractor(self, country_name, country_code, MAX_BBOX_AREA)
            
            try:
                # Force garbage collection before processing
                gc.collect()
                
                handler.apply_file(str(pbf_file), locations=True)
                
                # Save remaining addresses in final batch (if not at limit)
                if handler.addresses_batch and not handler.limit_reached:
                    # Check if final batch would exceed limit
                    remaining_capacity = handler.max_addresses - handler.total_saved
                    if remaining_capacity > 0:
                        # Only save up to the limit
                        addresses_to_save = handler.addresses_batch[:remaining_capacity]
                        if addresses_to_save:
                            self.save_addresses_batch(country_code, country_name, addresses_to_save)
                            handler.total_saved += len(addresses_to_save)
                
                if handler.limit_reached:
                    print(f"[Worker {self.worker_id}] Completed {country_code} with limit reached: {handler.total_saved} addresses")
                else:
                    print(f"[Worker {self.worker_id}] Completed {country_code}: {handler.total_saved} addresses")
                
            except Exception as e:
                error_msg = str(e)
                print(f"[Worker {self.worker_id}] Error processing PBF: {error_msg}")
                
                # Check if it's a memory error
                if "bad_alloc" in error_msg or "memory" in error_msg.lower():
                    print(f"[Worker {self.worker_id}] Memory error - file too large for system")
                    self.mark_skipped(country_code, "memory_error")
                else:
                    # If PBF processing fails for other reasons, delete the corrupted file
                    if pbf_file.exists():
                        print(f"[Worker {self.worker_id}] Deleting corrupted PBF file: {pbf_file}")
                        self.safe_remove_file(pbf_file)
                    self.release_country(country_code)
                return
            finally:
                # Clean up handler and force garbage collection
                if 'handler' in locals():
                    del handler
                gc.collect()
            
            # Step 3: Mark complete
            self.mark_complete(country_code)
            self.current_country = None
            
            # Keep PBF file (don't delete)
            print(f"[Worker {self.worker_id}] Keeping PBF file: {pbf_file}")
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

def save_missing_countries():
    """Generate missing countries report from MongoDB status collection"""
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        country_status_col = db.country_status
        
        # Find all skipped countries
        skipped_countries = list(country_status_col.find({"status": "skipped"}))
        
        if skipped_countries:
            print(f"\n=== MISSING COUNTRIES SUMMARY ===")
            print(f"Found {len(skipped_countries)} countries that were skipped:")
            
            missing_data = []
            for country in skipped_countries:
                reason = country.get('reason', 'unknown')
                print(f"  {country['country_code']} - {reason}")
                missing_data.append({
                    "country_code": country['country_code'],
                    "reason": reason,
                    "skipped_at": country.get('skipped_at', '').isoformat() if country.get('skipped_at') else ''
                })
            
            # Save to JSON file
            with open(MISSING_COUNTRIES_FILE, 'w', encoding='utf-8') as f:
                json.dump({
                    "skipped_countries": missing_data,
                    "total_count": len(missing_data),
                    "generated_at": datetime.utcnow().isoformat()
                }, f, indent=2, ensure_ascii=False)
            
            print(f"\nSkipped countries saved to: {MISSING_COUNTRIES_FILE}")
            print("Countries with 'no_geofabrik_url' need manual download")
        else:
            print("\n=== ALL COUNTRIES PROCESSED ===")
            print("No skipped countries found!")
        
        client.close()
    except Exception as e:
        print(f"Error generating missing countries report: {e}")

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global shutdown_requested
    print("\n[SIGNAL] Shutdown requested (Ctrl+C). Finishing current operation...")
    shutdown_requested = True

def main():
    global shutdown_requested
    
    if len(sys.argv) != 2:
        print("Usage: python worker_enhanced.py <worker_id>")
        sys.exit(1)
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # kill command
    
    worker_id = int(sys.argv[1])
    worker = EnhancedAddressWorker(worker_id)
    
    try:
        worker.run()
    except KeyboardInterrupt:
        print(f"\n[Worker {worker_id}] Interrupted by user")
    finally:
        worker.cleanup()
        save_missing_countries()  # Generate skipped countries report from MongoDB
        print(f"[Worker {worker_id}] Shutdown complete")

if __name__ == "__main__":
    main()