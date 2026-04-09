#!/usr/bin/env python
"""
Debug API search endpoint
"""

import requests
import json

BASE_URL = "http://127.0.0.1:8000"

print("\nTesting API /search endpoint:\n")

# Test 1: Simple search
query = "high temperature material"
url = f"{BASE_URL}/search?q={query}&top_k=5"

print(f"URL: {url}\n")

try:
    response = requests.get(url, timeout=30)
    print(f"Status Code: {response.status_code}")
    
    data = response.json()
    print(f"Response JSON:")
    print(json.dumps(data, indent=2))
    
    if 'items' in data:
        print(f"\nItems returned: {len(data.get('items', []))}")
        for item in data.get('items', [])[:3]:
            print(f"  - [{item.get('entity_type')}] {item.get('name')}")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

# Test 2: Try different query
print("\n" + "="*70)
print("\nTrying alternative query:\n")

queries = [
    "material temperature",
    "gene protein",
    "molecule drug",
    "soil",
]

for q in queries:
    try:
        r = requests.get(f"{BASE_URL}/search?q={q}&top_k=3", timeout=15)
        data = r.json()
        items = len(data.get('items', []))
        print(f"Query '{q}': {items} results")
    except:
        print(f"Query '{q}': ERROR")
