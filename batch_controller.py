#!/usr/bin/env python3
"""
Batch Controller for OSM Address Processing
Manages multiple workers and handles memory-intensive countries
"""

import json
import subprocess
import time
import sys
from pathlib import Path
from datetime import datetime
import signal
import threading
from typing import Dict, List

# Configuration
COUNTRIES_FILE = "geonames_countries.json"
BATCH_SIZE = 5  # Number of countries per batch
MAX_WORKERS = 3  # Number of parallel workers
WORKER_SCRIPT = "worker_memory_safe.py"
LOG_DIR = Path("./logs")

class BatchController:
    def __init__(self):
        self.running_workers = {}
        self.completed_countries = set()
        self.failed_countries = set()
        self.skipped_countries = set()
        self.shutdown_requested = False
        LOG_DIR.mkdir(exist_ok=True)
    
    def load_countries(self) -> Dict:
        """Load countries from JSON file"""
        with open(COUNTRIES_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def categorize_countries_by_size(self) -> Dict[str, List]:
        """Categorize countries by expected PBF file size"""
        countries = self.load_countries()
        
        # Rough categorization based on country characteristics
        small_countries = []    # < 50MB
        medium_countries = []   # 50-200MB  
        large_countries = []    # 200MB+
        
        # Small countries (islands, city-states, small nations)
        small_codes = {
            'AD', 'AI', 'AW', 'BB', 'BM', 'BQ', 'BV', 'CC', 'CK', 'CW', 'CX', 'DM', 'FK',
            'FO', 'GF', 'GG', 'GI', 'GL', 'GP', 'GS', 'GU', 'HM', 'IM', 'IO', 'JE', 'KI',
            'KY', 'LC', 'LI', 'MF', 'MH', 'MP', 'MQ', 'MS', 'MT', 'MU', 'MV', 'NC', 'NF',
            'NR', 'NU', 'PF', 'PM', 'PN', 'PR', 'PW', 'RE', 'SH', 'SJ', 'SM', 'TC', 'TF',
            'TK', 'TO', 'TV', 'UM', 'VA', 'VC', 'VG', 'VI', 'WF', 'WS', 'YT'
        }
        
        # Large countries (major nations with lots of data)
        large_codes = {
            'US', 'RU', 'CN', 'CA', 'BR', 'AU', 'IN', 'AR', 'KZ', 'DZ', 'CD', 'SA', 'MX',
            'ID', 'SD', 'LY', 'IR', 'MN', 'PE', 'TD', 'NE', 'AO', 'EG', 'TZ', 'ZA', 'CO',
            'ET', 'BO', 'MR', 'PK', 'VE', 'CL', 'TR', 'ZM', 'MM', 'AF', 'SO', 'CF', 'UA',
            'MG', 'BW', 'KE', 'FR', 'YE', 'TH', 'ES', 'TM', 'CM', 'PG', 'SW', 'UZ', 'IQ',
            'PY', 'ZW', 'JP', 'DE', 'MY', 'VN', 'FI', 'IT', 'PH', 'BF', 'NZ', 'GA', 'WE',
            'GM', 'GN', 'UK', 'UG', 'GH', 'RO', 'LA', 'GY', 'BY', 'KG', 'SN', 'SY', 'KH',
            'UR', 'TN', 'SL', 'BD', 'HN', 'ER', 'JO', 'GE', 'LB', 'NI', 'MK', 'MW', 'LR',
            'BJ', 'CU', 'GR', 'TG', 'IS', 'HU', 'PT', 'AZ', 'AT', 'CZ', 'PA', 'SZ', 'AE',
            'JM', 'AM', 'RW', 'TJ', 'AL', 'QA', 'NA', 'GM', 'LS', 'MK', 'SI', 'KW', 'FJ',
            'CY', 'TL', 'BH', 'VU', 'ME', 'EE', 'JM', 'TT', 'KM', 'LU', 'AD', 'MT', 'MV',
            'BN', 'IC', 'BS', 'BZ', 'CV', 'ST', 'WS', 'KI', 'PW', 'NR', 'TO', 'TV', 'VC'
        }
        
        for code, data in countries.items():
            if code in small_codes:
                small_countries.append((code, data))
            elif code in large_codes:
                large_countries.append((code, data))
            else:
                medium_countries.append((code, data))
        
        return {
            'small': small_countries,
            'medium': medium_countries,
            'large': large_countries
        }
    
    def start_worker(self, worker_id: int) -> subprocess.Popen:
        """Start a worker process"""
        log_file = LOG_DIR / f"worker_{worker_id}.log"
        
        with open(log_file, 'w') as f:
            process = subprocess.Popen(
                [sys.executable, WORKER_SCRIPT, str(worker_id)],
                stdout=f,
                stderr=subprocess.STDOUT,
                text=True
            )
        
        print(f"[Controller] Started worker {worker_id} (PID: {process.pid})")
        return process
    
    def monitor_workers(self):
        """Monitor running workers and restart if needed"""
        while not self.shutdown_requested:
            time.sleep(10)  # Check every 10 seconds
            
            for worker_id, process in list(self.running_workers.items()):
                if process.poll() is not None:  # Process finished
                    print(f"[Controller] Worker {worker_id} finished with code {process.returncode}")
                    del self.running_workers[worker_id]
                    
                    # Restart worker if not shutdown
                    if not self.shutdown_requested and len(self.running_workers) < MAX_WORKERS:
                        new_process = self.start_worker(worker_id)
                        self.running_workers[worker_id] = new_process
    
    def run_batch(self, batch_name: str, countries: List):
        """Run a batch of countries"""
        print(f"\n=== Starting {batch_name} batch ({len(countries)} countries) ===")
        
        # Start workers
        for i in range(min(MAX_WORKERS, len(countries))):
            worker_id = i + 1
            process = self.start_worker(worker_id)
            self.running_workers[worker_id] = process
        
        # Start monitoring thread
        monitor_thread = threading.Thread(target=self.monitor_workers, daemon=True)
        monitor_thread.start()
        
        # Wait for all countries to be processed
        start_time = time.time()
        while self.running_workers and not self.shutdown_requested:
            time.sleep(5)
            
            # Print status every minute
            if int(time.time() - start_time) % 60 == 0:
                elapsed = int(time.time() - start_time)
                print(f"[Controller] {batch_name} batch running for {elapsed//60}m {elapsed%60}s, {len(self.running_workers)} workers active")
        
        # Stop all workers
        self.stop_all_workers()
        
        elapsed = time.time() - start_time
        print(f"[Controller] {batch_name} batch completed in {elapsed//60:.0f}m {elapsed%60:.0f}s")
    
    def stop_all_workers(self):
        """Stop all running workers"""
        for worker_id, process in self.running_workers.items():
            try:
                print(f"[Controller] Stopping worker {worker_id}")
                process.terminate()
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                print(f"[Controller] Force killing worker {worker_id}")
                process.kill()
        
        self.running_workers.clear()
    
    def run_all_batches(self):
        """Run all batches in order of increasing size"""
        categories = self.categorize_countries_by_size()
        
        print(f"[Controller] Found {len(categories['small'])} small, {len(categories['medium'])} medium, {len(categories['large'])} large countries")
        
        # Process in order: small -> medium -> large
        for batch_name, countries in [
            ("SMALL", categories['small']),
            ("MEDIUM", categories['medium']),
            ("LARGE", categories['large'])
        ]:
            if countries and not self.shutdown_requested:
                self.run_batch(batch_name, countries)
                
                # Wait between batches
                if not self.shutdown_requested:
                    print(f"[Controller] Waiting 30 seconds before next batch...")
                    time.sleep(30)
        
        print(f"\n[Controller] All batches completed!")
    
    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        print(f"\n[Controller] Shutdown requested...")
        self.shutdown_requested = True
        self.stop_all_workers()

def main():
    controller = BatchController()
    
    # Register signal handlers
    signal.signal(signal.SIGINT, controller.signal_handler)
    signal.signal(signal.SIGTERM, controller.signal_handler)
    
    try:
        controller.run_all_batches()
    except KeyboardInterrupt:
        print(f"\n[Controller] Interrupted by user")
    finally:
        controller.stop_all_workers()
        print(f"[Controller] Shutdown complete")

if __name__ == "__main__":
    main()