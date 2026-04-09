#!/usr/bin/env python
"""
Interactive manual query tester for PRANA-G
Demonstrates different types of searches and queries
"""

import requests
import json
import time

BASE_URL = "http://127.0.0.1:8000"

def print_section(title):
    print("\n" + "="*70)
    print(f" {title}")
    print("="*70 + "\n")

def print_subsection(title):
    print(f"\n>>> {title}")
    print("-" * 70)

def make_request(endpoint, params):
    """Make HTTP request and return JSON response"""
    url = f"{BASE_URL}{endpoint}"
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code == 200:
            return r.json()
        else:
            return {"error": f"HTTP {r.status_code}", "message": r.text}
    except Exception as e:
        return {"error": str(e)}

def pretty_print(data, max_lines=30):
    """Pretty print JSON with line limit"""
    output = json.dumps(data, indent=2)
    lines = output.split('\n')
    if len(lines) > max_lines:
        print('\n'.join(lines[:max_lines]))
        print(f"\n... [{len(lines) - max_lines} more lines]")
    else:
        print(output)

# ============================================================================
print_section("PRANA-G INTERACTIVE QUERY DEMO")
print("Testing various search queries and API endpoints\n")

# Test 1: Health check
print_subsection("1. SYSTEM HEALTH CHECK")
print("Query: GET /health")
result = make_request("/health", {})
print(f"Status: {result.get('status', 'unknown')}")
print(f"Parquet Ready: {result.get('parquet_ready', False)}")
print(f"Vector Ready: {result.get('vector_ready', False)}")
print(f"Cache Backend: {result.get('cache', {}).get('backend', 'unknown')}")
print(f"Cache Rows: {result.get('cache', {}).get('rows_total', 0)}")

# Test 2: Simple search - materials
print_subsection("2. SEARCH: Materials Query")
print("Query: /search?q=high temperature material")
result = make_request("/search", {"q": "high temperature material", "top_k": 5})
print(f"Found {len(result.get('results', []))} results")
if result.get('results'):
    for i, r in enumerate(result['results'], 1):
        print(f"  {i}. [{r.get('entity_type', '?')}] {r.get('id', 'N/A')}")
        print(f"     Score: {r.get('score', 0):.4f}")
        if r.get('description'):
            print(f"     Desc: {r.get('description', '')[:60]}...")

# Test 3: Search - molecules
print_subsection("3. SEARCH: Molecules & Bioactivity")
print("Query: /search?q=drug bioactivity binding")
result = make_request("/search", {"q": "drug bioactivity binding", "top_k": 5})
print(f"Found {len(result.get('results', []))} results")
if result.get('results'):
    for i, r in enumerate(result['results'], 1):
        print(f"  {i}. [{r.get('entity_type', '?')}] {r.get('id', 'N/A')} - Score: {r.get('score', 0):.4f}")

# Test 4: Search - genes
print_subsection("4. SEARCH: Gene & Protein Sequences")
print("Query: /search?q=transcription factor expression")
result = make_request("/search", {"q": "transcription factor expression", "top_k": 5})
print(f"Found {len(result.get('results', []))} results")
if result.get('results'):
    for i, r in enumerate(result['results'], 1):
        print(f"  {i}. [{r.get('entity_type', '?')}] {r.get('id', 'N/A')} - Score: {r.get('score', 0):.4f}")

# Test 5: Location Context - Jodhpur
print_subsection("5. CONTEXT: Jodhpur, Rajasthan (26.3°N, 73.0°E)")
print("Query: /context?lat=26.3&lon=73.0&mode=auto")
result = make_request("/context", {"lat": 26.3, "lon": 73.0, "mode": "auto"})
if "error" not in result:
    print(f"Location: {result.get('location_name', 'Unknown')}")
    soil = result.get('soil', {})
    climate = result.get('climate', {})
    agri = result.get('agriculture', {})
    
    print(f"\nSoil:")
    print(f"  Type: {soil.get('type', '?')}")
    print(f"  pH: {soil.get('ph', '?')}")
    print(f"  Salinity: {soil.get('salinity', '?')}")
    
    print(f"\nClimate:")
    print(f"  Current Temp: {climate.get('temp_current', '?')}°C")
    print(f"  Max Temp: {climate.get('temp_max', '?')}°C")
    print(f"  Rainfall: {climate.get('rainfall', '?')}mm")
    
    print(f"\nAgriculture:")
    crops = agri.get('main_crops', [])
    print(f"  Main Crops: {', '.join(crops) if crops else 'N/A'}")
    print(f"  Irrigation: {agri.get('irrigation', '?')}")

# Test 6: Location Context - Different location
print_subsection("6. CONTEXT: Mumbai (19.08°N, 72.88°E)")
print("Query: /context?lat=19.08&lon=72.88&mode=auto")
result = make_request("/context", {"lat": 19.08, "lon": 72.88, "mode": "auto"})
if "error" not in result:
    print(f"Location: {result.get('location_name', 'Unknown')}")
    soil = result.get('soil', {})
    climate = result.get('climate', {})
    
    print(f"Soil Type: {soil.get('type', '?')}")
    print(f"Temp Current: {climate.get('temp_current', '?')}°C")
    print(f"Rainfall: {climate.get('rainfall', '?')}mm")

# Test 7: Recommendation - compound query
print_subsection("7. RECOMMENDATION: Desert Material Design")
print("Query: /recommend with location context")
print("Request: Design self-healing material for Jodhpur desert")
result = make_request("/recommend", {
    "q": "self healing high temperature material for desert",
    "lat": 26.3,
    "lon": 73.0,
    "context_mode": "auto",
    "top_k": 3
})
print(f"Recommendations returned: {len(result.get('recommendations', []))}")
if result.get('recommendations'):
    for i, combo in enumerate(result['recommendations'], 1):
        print(f"\n  Combo {i}:")
        print(f"    Overall Score: {combo.get('score', 0):.4f}")
        entities = combo.get('entity_keys', [])
        print(f"    Entities suggested ({len(entities)}):")
        for entity in entities[:5]:
            print(f"      - {entity}")

# Test 8: Recommendation - agricultural use case
print_subsection("8. RECOMMENDATION: Agricultural Resilience")
print("Query: /recommend for crop resilience")
result = make_request("/recommend", {
    "q": "drought resistant crop gene for Rajasthan agriculture",
    "lat": 26.3,
    "lon": 73.0,
    "top_k": 3
})
print(f"Recommendations: {len(result.get('recommendations', []))}")
if result.get('recommendations'):
    for i, combo in enumerate(result['recommendations'][:2], 1):
        print(f"  {i}. Score: {combo.get('score', 0):.4f}, Entities: {len(combo.get('entity_keys', []))}")

# Test 9: Metrics
print_subsection("9. API METRICS")
print("Query: /metrics")
result = make_request("/metrics", {})
print(f"Total Requests: {result.get('requests_total', 0)}")
print(f"Total Errors: {result.get('errors_total', 0)}")
print(f"Avg Response Time: {result.get('average_request_duration_ms', 0):.2f}ms")
print(f"\nEndpoint Breakdown:")
for path, count in result.get('path_counters', {}).items():
    print(f"  {path}: {count} calls")

# Test 10: Pipeline State
print_subsection("10. PIPELINE STATE")
print("Query: /ops/state")
result = make_request("/ops/state", {})
if "error" not in result:
    pretty_print(result, max_lines=20)
else:
    print(f"Error: {result.get('error', 'Unknown')}")

# Test 11: Ingestion Status
print_subsection("11. INGESTION STATUS")
print("Query: /ingestion/status")
result = make_request("/ingestion/status", {})
summary = result.get('summary', {})
print(f"Run ID: {summary.get('run_id', 'N/A')}")
print(f"Status: {summary.get('status', '?')}")
print(f"Rows Total: {summary.get('rows_total', '?')}")
if summary.get('rows_by_type'):
    print("Rows by Type:")
    for entity_type, count in summary.get('rows_by_type', {}).items():
        print(f"  {entity_type}: {count}")

# Test 12: Literature Status
print_subsection("12. LITERATURE STATUS")
print("Query: /literature/status")
result = make_request("/literature/status", {})
summary = result.get('summary', {})
print(f"Total Papers Processed: {summary.get('total_papers', 0)}")
print(f"Total Entities Extracted: {summary.get('total_entities_extracted', 0)}")
print(f"Status: {summary.get('status', '?')}")

# Test 13: Multi-domain search
print_subsection("13. MULTI-DOMAIN SEARCH: Complex Query")
print("Query: /search?q=quantum computing materials DNA")
result = make_request("/search", {"q": "quantum computing materials DNA applications", "top_k": 10})
domains = {}
for r in result.get('results', []):
    entity_type = r.get('entity_type', 'unknown')
    domains[entity_type] = domains.get(entity_type, 0) + 1

print(f"Total Results: {len(result.get('results', []))}")
print(f"Domains Covered:")
for domain, count in sorted(domains.items()):
    print(f"  {domain}: {count} results")

print_section("DEMO COMPLETE")
print("\nFull API Documentation:")
print("  → http://127.0.0.1:8000/docs (Swagger UI)")
print("  → http://127.0.0.1:8000/redoc (ReDoc UI)")
print("\nTo use live queries, copy any of the test queries above")
print("or visit the Swagger UI and test interactively.\n")
