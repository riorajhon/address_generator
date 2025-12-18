#!/usr/bin/env python3
"""
Batch Runner for OSM Processing with Worker ID Support
Handles huge countries without std::bad_alloc errors
Usage: python run_batch.py [worker_id] [max_batches]
"""

import json
import subprocess
import sys
import time
import gc
import os
from pathlib import Path
from datetime import datetime

# Try to import memory monitoring
try:
    import psutil
    MEMORY_MONITORING = True
except ImportError:
    MEMORY_MONITORING = False

def check_system_memory():
    """Check available system memory"""
    if MEMORY_MONITORING:
        mem = psutil.virtual_memory()
        return {
            'total_gb': mem.total / (1024**3),
            'available_gb': mem.available / (1024**3),
            'used_percent': mem.percent,
            'safe_to_process': mem.percent < 80 and mem.available > 2 * (1024**3)  # 2GB minimum
        }
    else:
        return {
            'total_gb': 'unknown',
            'available_gb': 'unknown', 
            'used_percent': 'unknown',
            'safe_to_process': True  # Assume safe if can't check
        }

def run_single_worker(worker_id: int, timeout_minutes: int = 120):
    """Run a single worker and wait for completion"""
    print(f"[Batch] Starting worker {worker_id}...")
    
    # Check memory before starting
    mem_info = check_system_memory()
    if not mem_info['safe_to_process']:
        print(f"[Batch] Warning: High memory usage ({mem_info['used_percent']}%), proceeding with caution")
    
    try:
        # Force garbage collection before starting
        gc.collect()
        
        # Run worker with real-time output
        log_file = Path(f"logs/batch_worker_{worker_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        log_file.parent.mkdir(exist_ok=True)
        
        print(f"[Batch] Logging to: {log_file}")
        
        with open(log_file, 'w') as f:
            process = subprocess.Popen(
                [sys.executable, "worker_memory_safe.py", str(worker_id)],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True
            )
            
            # Read output in real-time
            start_time = time.time()
            timeout_seconds = timeout_minutes * 60
            
            while True:
                # Check if process finished
                if process.poll() is not None:
                    break
                
                # Check timeout
                if time.time() - start_time > timeout_seconds:
                    print(f"[Batch] Worker {worker_id} timed out after {timeout_minutes} minutes")
                    process.terminate()
                    try:
                        process.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        process.kill()
                    return False
                
                # Read output line by line
                try:
                    line = process.stdout.readline()
                    if line:
                        print(f"[Worker {worker_id}] {line.rstrip()}")
                        f.write(line)
                        f.flush()
                    else:
                        time.sleep(0.1)
                except:
                    break
        
        return_code = process.returncode
        elapsed_minutes = (time.time() - start_time) / 60
        
        print(f"[Batch] Worker {worker_id} completed in {elapsed_minutes:.1f} minutes with return code: {return_code}")
        
        # Force cleanup
        gc.collect()
        
        return return_code == 0
        
    except Exception as e:
        print(f"[Batch] Error running worker {worker_id}: {e}")
        return False

def get_remaining_countries():
    """Check how many countries are left to process"""
    try:
        from pymongo import MongoClient
        
        mongo_uri = os.getenv("MONGO_URI", "mongodb://admin:wkrjk!20020415@localhost:27017/?authSource=admin")
        client = MongoClient(mongo_uri)
        db = client["address_db"]
        
        # Load total countries
        with open("geonames_countries.json", 'r', encoding='utf-8') as f:
            total_countries = len(json.load(f))
        
        # Count processed countries
        processed = db.country_status.count_documents({
            "status": {"$in": ["completed", "skipped"]}
        })
        
        remaining = total_countries - processed
        client.close()
        
        return remaining, total_countries, processed
        
    except Exception as e:
        print(f"[Batch] Could not check remaining countries: {e}")
        return None, None, None

def main():
    """Run workers in batches with memory management"""
    # Parse command line arguments
    worker_id = 1
    max_batches = 50
    
    if len(sys.argv) > 1:
        try:
            worker_id = int(sys.argv[1])
        except ValueError:
            print("Error: worker_id must be an integer")
            sys.exit(1)
    
    if len(sys.argv) > 2:
        try:
            max_batches = int(sys.argv[2])
        except ValueError:
            print("Error: max_batches must be an integer")
            sys.exit(1)
    
    print("=== OSM Batch Processing for Huge Countries ===")
    print(f"Worker ID: {worker_id}")
    print(f"Max Batches: {max_batches}")
    
    # Check system info
    mem_info = check_system_memory()
    if MEMORY_MONITORING:
        print(f"System Memory: {mem_info['total_gb']:.1f}GB total, {mem_info['available_gb']:.1f}GB available ({mem_info['used_percent']:.1f}% used)")
    else:
        print("Memory monitoring not available (install psutil for better monitoring)")
    
    # Check if required files exist
    required_files = ["worker_memory_safe.py", "geonames_countries.json", "looks_like_address.py"]
    for file in required_files:
        if not Path(file).exists():
            print(f"Error: {file} not found")
            return
    
    print(f"All required files found ✓")
    print()
    
    # Create logs directory
    Path("logs").mkdir(exist_ok=True)
    
    batch_count = 0
    successful_batches = 0
    failed_batches = 0
    
    try:
        while batch_count < max_batches:
            batch_count += 1
            
            # Check remaining countries
            remaining, total, processed = get_remaining_countries()
            if remaining is not None:
                print(f"\n--- Batch {batch_count} (Progress: {processed}/{total}, {remaining} remaining) ---")
                if remaining == 0:
                    print("[Batch] All countries processed! 🎉")
                    break
            else:
                print(f"\n--- Batch {batch_count} ---")
            
            # Check memory before batch
            mem_info = check_system_memory()
            if MEMORY_MONITORING and not mem_info['safe_to_process']:
                print(f"[Batch] Waiting for memory to free up... (currently {mem_info['used_percent']:.1f}% used)")
                time.sleep(30)
                continue
            
            # Run worker
            success = run_single_worker(worker_id, timeout_minutes=180)  # 3 hour timeout for huge countries
            
            if success:
                successful_batches += 1
                print(f"[Batch] Batch {batch_count} completed successfully ✅")
            else:
                failed_batches += 1
                print(f"[Batch] Batch {batch_count} failed ❌")
                
                # If multiple failures, increase delay
                if failed_batches >= 3:
                    print(f"[Batch] Multiple failures detected, waiting 60 seconds...")
                    time.sleep(60)
            
            # Memory cleanup between batches
            gc.collect()
            
            # Adaptive delay based on system load
            if MEMORY_MONITORING:
                mem = psutil.virtual_memory()
                if mem.percent > 70:
                    delay = 30
                elif mem.percent > 50:
                    delay = 15
                else:
                    delay = 5
            else:
                delay = 10
            
            print(f"[Batch] Waiting {delay} seconds before next batch...")
            time.sleep(delay)
    
    except KeyboardInterrupt:
        print(f"\n[Batch] Interrupted by user")
    except Exception as e:
        print(f"\n[Batch] Unexpected error: {e}")
    
    # Final summary
    print(f"\n=== Batch Processing Complete ===")
    print(f"Total batches run: {batch_count}")
    print(f"Successful: {successful_batches}")
    print(f"Failed: {failed_batches}")
    
    if remaining is not None:
        print(f"Countries remaining: {remaining}")
    
    print(f"Logs saved in: logs/")

if __name__ == "__main__":
    main()