#!/usr/bin/env python
"""
Simple manual query test with proper timeout handling
"""

import requests
import json
import sys
from typing import Optional

class SearchTester:
    def __init__(self, base_url: str = "http://127.0.0.1:8000"):
        self.base_url = base_url
        self.session = requests.Session()
        # Set longer timeout for first semantic search (embedding model load)
        self.timeout = 120
        
    def test_search(self, query: str, top_k: int = 5) -> Optional[dict]:
        """Test search endpoint"""
        try:
            print(f"\n🔍 SEARCHING: '{query}'")
            print(f"   URL: {self.base_url}/search?q={query}&top_k={top_k}")
            print(f"   Timeout: {self.timeout}s (first search loads embedding model)")
            
            # Use extended timeout for first request
            response = self.session.get(
                f"{self.base_url}/search?q={query}&top_k={top_k}",
                timeout=self.timeout
            )
            
            if response.status_code != 200:
                print(f"   ✗ HTTP {response.status_code}: {response.text}")
                return None
                
            data = response.json()
            print(f"   ✓ {len(data.get('items', []))} results found")
            
            # Reduce timeout for subsequent requests
            self.timeout = 30
            return data
            
        except requests.Timeout:
            print(f"   ✗ Request timed out after {self.timeout}s")
            return None
        except Exception as e:
            print(f"   ✗ Error: {e}")
            return None
    
    def display_results(self, data: dict):
        """Pretty print results"""
        items = data.get('items', [])
        if not items:
            print("   (No results)")
            return
            
        for i, item in enumerate(items[:5], 1):
            print(f"\n   [{i}] {item.get('entity_type', '?').upper()}")
            print(f"       Name: {item.get('name', 'N/A')}")
            if item.get('description'):
                desc = item['description'][:80]
                print(f"       Desc: {desc}...")
            print(f"       Source: {item.get('source', 'N/A')}")

# ============================================================================
print("="*70)
print(" PRANA-G MANUAL QUERY TEST")
print("="*70)

tester = SearchTester()

# Test 1: High temperature material
print("\n" + "-"*70)
print("MANUAL QUERY 1: Materials for extreme conditions")
print("-"*70)

result = tester.test_search("high temperature thermal resistant material", top_k=5)
if result:
    tester.display_results(result)

# Test 2: Biological keywords
print("\n" + "-"*70)
print("MANUAL QUERY 2: Biological entities")
print("-"*70)

result = tester.test_search("gene protein expression transcription", top_k=5)
if result:
    tester.display_results(result)

# Test 3: Chemical/molecular
print("\n" + "-"*70)
print("MANUAL QUERY 3: Molecular entities")
print("-"*70)

result = tester.test_search("molecule drug compound bioactivity", top_k=5)
if result:
    tester.display_results(result)

# Test 4: Environmental
print("\n" + "-"*70)
print("MANUAL QUERY 4: Environmental/Agricultural")
print("-"*70)

result = tester.test_search("soil farming agriculture irrigation crop", top_k=5)
if result:
    tester.display_results(result)

# Test 5: Context-aware location query
print("\n" + "-"*70)
print("MANUAL QUERY 5: Context Lookup - multiple locations")
print("-"*70)

locations = [
    ("Jodhpur, Rajasthan", 26.3, 73.0),
    ("Silicon Valley", 37.4, -122.1),
    ("Mumbai", 19.08, 72.88),
]

for name, lat, lon in locations:
    try:
        print(f"\n📍 {name} ({lat}°, {lon}°)")
        r = requests.get(
            f"http://127.0.0.1:8000/context?lat={lat}&lon={lon}&mode=auto",
            timeout=10
        )
        if r.status_code == 200:
            data = r.json()
            print(f"   ✓ Found: {data.get('location_name', 'Unknown')}")
            soil = data.get('soil', {})
            climate = data.get('climate', {})
            print(f"   • Soil: {soil.get('type', '?')} (pH: {soil.get('ph', '?')})")
            print(f"   • Temp: {climate.get('temp_current', '?')}°C (max: {climate.get('temp_max', '?')}°C)")
            print(f"   • Rainfall: {climate.get('rainfall', '?')}mm")
        else:
            print(f"   ✗ HTTP {r.status_code}")
    except Exception as e:
        print(f"   ✗ Error: {e}")

print("\n" + "="*70)
print("TEST COMPLETE")
print("="*70)
print("\nNotes:")
print("- First /search query loads embedding model (may take 20-60s)")
print("- Subsequent queries are cached and respond faster")
print("- Full API docs: http://127.0.0.1:8000/docs")
print("="*70 + "\n")
