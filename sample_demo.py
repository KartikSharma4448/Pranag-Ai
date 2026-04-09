#!/usr/bin/env python
"""
Sample demo queries for PRANA-G Universal Scientific Index
"""

import requests
import json
import time

time.sleep(3)  # Wait for API to fully start

base_url = 'http://127.0.0.1:8000'

print('\n' + '='*60)
print('PRANA-G SAMPLE DEMO')
print('='*60 + '\n')

# Sample 1: Health check
print('✓ API Health Check')
r = requests.get(f'{base_url}/health')
if r.status_code == 200:
    print(f'  Status: {r.json()}\n')

# Sample 2: Search query
print('SAMPLE 1: SEMANTIC SEARCH')
print('-' * 60)
print('Query: "self healing high temperature material"')
r = requests.get(f'{base_url}/search', params={'q': 'self healing high temperature material'})
if r.status_code == 200:
    data = r.json()
    print(f'Found {len(data.get("results", []))} results across domains:')
    for i, result in enumerate(data.get('results', [])[:5], 1):
        print(f'  {i}. [{result.get("entity_type", "?")}] {result.get("id", "")} - Score: {result.get("score", 0):.3f}')
else:
    print(f'Error: {r.status_code}')

# Sample 3: Context query
print('\nSAMPLE 2: ENVIRONMENTAL CONTEXT')
print('-' * 60)
print('Location: Lat=26.3, Lon=73.0 (Jodhpur, Rajasthan)')
r = requests.get(f'{base_url}/context', params={'lat': 26.3, 'lon': 73.0, 'mode': 'auto'})
if r.status_code == 200:
    data = r.json()
    print(f'Climate Zone: {data.get("climate_zone", "?")}')
    print(f'Soil Type: {data.get("soil_type", "?")}')
    print(f'Agricultural Suitability: {data.get("agricultural_suitability", "?")}')
    print(json.dumps(data, indent=2)[:500] + '...')
else:
    print(f'Error: {r.status_code}')

# Sample 4: Recommendation query
print('\nSAMPLE 3: CROSS-DOMAIN RECOMMENDATION')
print('-' * 60)
query = 'Design a self healing high temperature material for Rajasthan desert'
print(f'Query: "{query}"')
print('Location: Jodhpur, Rajasthan (26.3°N, 73.0°E)')
r = requests.get(f'{base_url}/recommend', params={
    'q': query,
    'lat': 26.3,
    'lon': 73.0,
    'context_mode': 'auto'
})
if r.status_code == 200:
    data = r.json()
    print(f'\nRecommendations:')
    for combo in data.get('recommendations', [])[:3]:
        print(f'  • Score: {combo.get("score", 0):.3f}')
        for key in combo.get('entity_keys', []):
            print(f'    - {key}')
else:
    print(f'Error: {r.status_code}')

# Sample 5: System metrics
print('\nSAMPLE 4: SYSTEM METRICS')
print('-' * 60)
r = requests.get(f'{base_url}/metrics')
if r.status_code == 200:
    data = r.json()
    print(json.dumps(data, indent=2))
else:
    print(f'Error: {r.status_code}')

print('\n' + '='*60)
print('Demo Complete! Access full API docs at:')
print('http://127.0.0.1:8000/docs')
print('='*60 + '\n')
