#!/usr/bin/env python3
"""
OSM Address Extractor Worker
Processes countries from geonames_countries.json in parallel across 50 workers
Usage: python worker.py <worker_id>
"""

import json
import os
import sys
import signal
from pathlib import Path
from typing import Optional, Dict, List
import urllib.request
from pymongo import MongoClient, ASCENDING
from datetime import datetime
import osmium
from looks_like_address import looks_like_address

# Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:wkrjk!20020415@localhost:27017/?authSource=admin")
DB_NAME = "address_db"
COUNTRIES_FILE = "geonames_countries.json"
WORK_DIR = Path("./osm_data")
MAX_BBOX_AREA = 100  # m^2

# Global flag for graceful shutdown
shutdown_requested = False

class AddressExtractor(osmium.SimpleHandler):
    """Extract buildings with full addresses from OSM PBF"""
    
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
        
        if shutdown_requested:
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
    
    def way(self, w):
        """Process each way (building)"""
        global shutdown_requested
        
        self.processed += 1
        
        # Progress logging
        if self.processed % 1000 == 0:
            print(f"[Worker {self.worker.worker_id}] Processed {self.processed} ways, found {self.found} addresses")
        
        if shutdown_requested:
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

class AddressWorker:
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
        """Claim next available country for processing"""
        with open(COUNTRIES_FILE, 'r', encoding='utf-8') as f:
            countries = json.load(f)
        
        for country_code in countries.keys():
            # Try to atomically claim this country
            result = self.country_status_col.find_one_and_update(
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
            
            # If result is None, we successfully claimed this country
            if result is None:
                return country_code
        
        return None
    
    def download_pbf(self, country_code: str, country_name: str) -> Optional[Path]:
        """Download OSM PBF file for country"""
        from geofabrik_urls import get_geofabrik_url
        
        pbf_file = WORK_DIR / f"{country_code.lower()}-latest.osm.pbf"
        
        if pbf_file.exists():
            print(f"[Worker {self.worker_id}] PBF already exists: {pbf_file}")
            return pbf_file
        
        # Get correct Geofabrik URL for this country
        primary_url = get_geofabrik_url(country_code, country_name)
        
        # Try primary URL first, then fallbacks
        urls = [primary_url]
        
        # Add fallback URLs in case primary fails
        country_slug = country_name.lower().replace(' ', '-').replace('&', 'and')
        urls.extend([
            f"https://download.geofabrik.de/{country_slug}-latest.osm.pbf",
            f"https://download.geofabrik.de/{country_code.upper()}-latest.osm.pbf",
            f"https://download.geofabrik.de/{country_code.lower()}-latest.osm.pbf"
        ])
        
        for url in urls:
            try:
                print(f"[Worker {self.worker_id}] Downloading {url}...")
                urllib.request.urlretrieve(url, pbf_file)
                
                # Validate PBF file
                if pbf_file.exists() and pbf_file.stat().st_size > 1000:  # At least 1KB
                    # Try to read first few bytes to validate PBF format
                    with open(pbf_file, 'rb') as f:
                        header = f.read(4)
                        if header and len(header) == 4:
                            print(f"[Worker {self.worker_id}] Downloaded: {pbf_file} ({pbf_file.stat().st_size} bytes)")
                            return pbf_file
                
                # If validation fails, delete and try next URL
                print(f"[Worker {self.worker_id}] Downloaded file appears corrupted, trying next URL...")
                pbf_file.unlink(missing_ok=True)
                
            except Exception as e:
                print(f"[Worker {self.worker_id}] Failed to download from {url}: {e}")
                pbf_file.unlink(missing_ok=True)
                continue
        
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
        
        # Cleanup only temporary files (not original PBF files)
        if WORK_DIR.exists():
            for temp_file in WORK_DIR.glob("*.filtered.osm.pbf"):
                temp_file.unlink(missing_ok=True)
            for temp_file in WORK_DIR.glob("*.json"):
                temp_file.unlink(missing_ok=True)
        
        # Close MongoDB connection
        self.client.close()
        print(f"[Worker {self.worker_id}] Cleanup complete")
    
    def process_country(self, country_code: str, country_data: Dict):
        """Process a single country"""
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
                print(f"[Worker {self.worker_id}] Failed to download PBF for {country_code}")
                self.release_country(country_code)
                return
            
            self.current_pbf_file = pbf_file
            
            # Step 2: Extract addresses using osmium
            if shutdown_requested:
                return
            
            print(f"[Worker {self.worker_id}] Extracting addresses from {pbf_file}")
            handler = AddressExtractor(self, country_name, country_code, MAX_BBOX_AREA)
            
            try:
                handler.apply_file(str(pbf_file), locations=True)
                
                # Save remaining addresses in final batch
                if handler.addresses_batch:
                    self.save_addresses_batch(country_code, country_name, handler.addresses_batch)
                    handler.total_saved += len(handler.addresses_batch)
                
                print(f"[Worker {self.worker_id}] Saved {handler.total_saved} addresses for {country_code}")
                
            except Exception as e:
                print(f"[Worker {self.worker_id}] Error processing PBF: {e}")
                self.release_country(country_code)
                return
            
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

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global shutdown_requested
    print("\n[SIGNAL] Shutdown requested (Ctrl+C). Finishing current operation...")
    shutdown_requested = True

def main():
    global shutdown_requested
    
    if len(sys.argv) != 2:
        print("Usage: python worker.py <worker_id>")
        sys.exit(1)
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # kill command
    
    worker_id = int(sys.argv[1])
    worker = AddressWorker(worker_id)
    
    try:
        worker.run()
    except KeyboardInterrupt:
        print(f"\n[Worker {worker_id}] Interrupted by user")
    finally:
        worker.cleanup()
        print(f"[Worker {worker_id}] Shutdown complete")

if __name__ == "__main__":
    main()