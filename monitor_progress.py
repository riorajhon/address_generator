#!/usr/bin/env python3
"""
Progress Monitor for OSM Address Processing
Shows current status of country processing
"""

import json
import os
from pymongo import MongoClient
from datetime import datetime
from pathlib import Path

# Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:wkrjk!20020415@localhost:27017/?authSource=admin")
DB_NAME = "address_db"
COUNTRIES_FILE = "geonames_countries.json"

def get_processing_status():
    """Get current processing status from MongoDB"""
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        country_status_col = db.country_status
        addresses_col = db.address
        
        # Get status counts
        status_counts = {}
        for status in ["processing", "completed", "skipped"]:
            count = country_status_col.count_documents({"status": status})
            status_counts[status] = count
        
        # Get total countries
        with open(COUNTRIES_FILE, 'r', encoding='utf-8') as f:
            total_countries = len(json.load(f))
        
        # Get address counts
        total_addresses = addresses_col.count_documents({})
        
        # Get countries by status
        processing = list(country_status_col.find({"status": "processing"}))
        completed = list(country_status_col.find({"status": "completed"}).limit(10))
        skipped = list(country_status_col.find({"status": "skipped"}).limit(10))
        
        client.close()
        
        return {
            "total_countries": total_countries,
            "status_counts": status_counts,
            "total_addresses": total_addresses,
            "processing": processing,
            "completed": completed,
            "skipped": skipped
        }
        
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None

def print_status():
    """Print current processing status"""
    status = get_processing_status()
    
    if not status:
        return
    
    print("=== OSM Address Processing Status ===")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Overall progress
    total = status["total_countries"]
    completed = status["status_counts"].get("completed", 0)
    processing = status["status_counts"].get("processing", 0)
    skipped = status["status_counts"].get("skipped", 0)
    remaining = total - completed - processing - skipped
    
    progress_pct = (completed / total * 100) if total > 0 else 0
    
    print(f"Overall Progress: {completed}/{total} ({progress_pct:.1f}%)")
    print(f"  ✅ Completed: {completed}")
    print(f"  🔄 Processing: {processing}")
    print(f"  ⏭️  Skipped: {skipped}")
    print(f"  ⏳ Remaining: {remaining}")
    print()
    
    # Address count
    print(f"Total Addresses Extracted: {status['total_addresses']:,}")
    print()
    
    # Currently processing
    if status["processing"]:
        print("Currently Processing:")
        for country in status["processing"]:
            started = country.get("started_at", "Unknown")
            if isinstance(started, datetime):
                started = started.strftime('%H:%M:%S')
            print(f"  🔄 {country['country_code']} (Worker {country.get('worker_id', '?')}, started: {started})")
        print()
    
    # Recently completed
    if status["completed"]:
        print("Recently Completed:")
        for country in status["completed"][:5]:
            completed_at = country.get("completed_at", "Unknown")
            if isinstance(completed_at, datetime):
                completed_at = completed_at.strftime('%H:%M:%S')
            
            # Check if it was completed with 500k limit
            completion_type = country.get("completion_type", "unknown")
            addresses_saved = country.get("addresses_saved", "unknown")
            limit_reached = country.get("limit_reached", False)
            
            if limit_reached:
                print(f"  ✅ {country['country_code']} (500k limit reached, {addresses_saved} addresses, {completed_at})")
            else:
                print(f"  ✅ {country['country_code']} (fully processed, {completed_at})")
        print()
    
    # Skipped countries
    if status["skipped"]:
        print("Skipped Countries:")
        for country in status["skipped"][:5]:
            reason = country.get("reason", "unknown")
            print(f"  ⏭️  {country['country_code']} ({reason})")
        if len(status["skipped"]) > 5:
            print(f"  ... and {len(status['skipped']) - 5} more")
        print()

def main():
    """Main function"""
    try:
        print_status()
    except KeyboardInterrupt:
        print("\nMonitoring stopped")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()