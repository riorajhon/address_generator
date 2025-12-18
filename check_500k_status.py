#!/usr/bin/env python3
"""
Check 500k Address Limit Status
Shows which countries reached the 500k limit vs fully processed
"""

import os
from pymongo import MongoClient
from datetime import datetime

# Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:wkrjk!20020415@localhost:27017/?authSource=admin")
DB_NAME = "address_db"

def check_500k_status():
    """Check status of countries with 500k limit"""
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        country_status_col = db.country_status
        addresses_col = db.address
        
        # Get countries that reached 500k limit
        limit_reached = list(country_status_col.find({
            "status": "completed",
            "limit_reached": True
        }).sort("completed_at", -1))
        
        # Get countries fully processed
        fully_processed = list(country_status_col.find({
            "status": "completed",
            "$or": [
                {"limit_reached": {"$exists": False}},
                {"limit_reached": False}
            ]
        }).sort("completed_at", -1))
        
        # Get address counts per country
        address_counts = {}
        pipeline = [
            {"$group": {"_id": "$country", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        
        for result in addresses_col.aggregate(pipeline):
            address_counts[result["_id"]] = result["count"]
        
        client.close()
        
        print("=== 500K Address Limit Status ===")
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # Countries that reached 500k limit
        if limit_reached:
            print(f"🎯 Countries with 500K Limit Reached ({len(limit_reached)}):")
            for country in limit_reached:
                country_code = country['country_code']
                addresses_saved = country.get('addresses_saved', 'unknown')
                completed_at = country.get('completed_at', 'unknown')
                actual_count = address_counts.get(country_code, 0)
                
                if isinstance(completed_at, datetime):
                    completed_at = completed_at.strftime('%Y-%m-%d %H:%M')
                
                print(f"  🎯 {country_code}: {addresses_saved} saved, {actual_count:,} in DB (completed: {completed_at})")
            print()
        
        # Countries fully processed
        if fully_processed:
            print(f"✅ Countries Fully Processed ({len(fully_processed)}):")
            for country in fully_processed[:10]:  # Show top 10
                country_code = country['country_code']
                completed_at = country.get('completed_at', 'unknown')
                actual_count = address_counts.get(country_code, 0)
                
                if isinstance(completed_at, datetime):
                    completed_at = completed_at.strftime('%Y-%m-%d %H:%M')
                
                print(f"  ✅ {country_code}: {actual_count:,} addresses (completed: {completed_at})")
            
            if len(fully_processed) > 10:
                print(f"  ... and {len(fully_processed) - 10} more")
            print()
        
        # Top countries by address count
        print("🏆 Top Countries by Address Count:")
        top_countries = sorted(address_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        for i, (country_code, count) in enumerate(top_countries, 1):
            # Check if this country reached limit
            limit_status = ""
            for country in limit_reached:
                if country['country_code'] == country_code:
                    limit_status = " (500k limit)"
                    break
            
            print(f"  {i:2d}. {country_code}: {count:,} addresses{limit_status}")
        
        print()
        
        # Summary
        total_with_limit = len(limit_reached)
        total_fully_processed = len(fully_processed)
        total_addresses = sum(address_counts.values())
        
        print("📊 Summary:")
        print(f"  Countries with 500k limit: {total_with_limit}")
        print(f"  Countries fully processed: {total_fully_processed}")
        print(f"  Total addresses in database: {total_addresses:,}")
        
        if total_with_limit > 0:
            estimated_500k_addresses = total_with_limit * 500000
            print(f"  Estimated addresses from 500k countries: {estimated_500k_addresses:,}")
        
    except Exception as e:
        print(f"Error: {e}")

def main():
    check_500k_status()

if __name__ == "__main__":
    main()