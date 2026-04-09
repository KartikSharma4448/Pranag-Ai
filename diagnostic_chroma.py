#!/usr/bin/env python
"""
Diagnostic script to check ChromaDB status and troubleshoot vector search
"""

import chromadb
from pathlib import Path

CHROMA_DIR = Path("data/processed/chroma")
COLLECTION_NAME = "universal_index_day2"

print("\n" + "="*70)
print("ChromaDB DIAGNOSTIC CHECK")
print("="*70 + "\n")

# Check if chroma directory exists
print(f"1. Checking ChromaDB directory: {CHROMA_DIR}")
if CHROMA_DIR.exists():
    print(f"   ✓ Directory exists")
    print(f"   Contents: {list(CHROMA_DIR.iterdir())}")
else:
    print(f"   ✗ Directory doesn't exist")
    exit(1)

# Connect to ChromaDB
print(f"\n2. Connecting to ChromaDB...")
try:
    client = chromadb.PersistentClient(path=str(CHROMA_DIR))
    print(f"   ✓ Connected")
except Exception as e:
    print(f"   ✗ Failed to connect: {e}")
    exit(1)

# List collections
print(f"\n3. Available collections:")
collections = client.list_collections()
print(f"   Found {len(collections)} collection(s)")
for col in collections:
    print(f"     - {col.name} (count: {col.count()})")

# Get our collection
print(f"\n4. Accessing collection '{COLLECTION_NAME}'...")
try:
    collection = client.get_collection(name=COLLECTION_NAME)
    print(f"   ✓ Collection found")
    print(f"   Document count: {collection.count()}")
except Exception as e:
    print(f"   ✗ Collection not found: {e}")
    print(f"   Available collections: {[c.name for c in collections]}")
    exit(1)

# Try a simple query
print(f"\n5. Testing semantic query...")
try:
    # Get an existing embedding to test
    results = collection.get(limit=5)
    if results and results['ids']:
        print(f"   ✓ Collection has data")
        print(f"   Sample IDs: {results['ids'][:3]}")
    else:
        print(f"   ✗ Collection is empty")
except Exception as e:
    print(f"   ✗ Query failed: {e}")

# Try actual semantic search
print(f"\n6. Testing with embedding model...")
try:
    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
    query_text = "high temperature material"
    embedding = model.encode([query_text], normalize_embeddings=True)[0].tolist()
    
    results = collection.query(
        query_embeddings=[embedding],
        n_results=5,
        include=["documents", "metadatas", "distances"]
    )
    
    print(f"   ✓ Model loaded and embedding created")
    print(f"   Results for '{query_text}':")
    
    if results and results['ids'] and results['ids'][0]:
        for i, (id_, doc, meta, dist) in enumerate(zip(
            results['ids'][0],
            results['documents'][0],
            results['metadatas'][0],
            results['distances'][0]
        ), 1):
            print(f"     {i}. [{meta.get('entity_type', '?')}] {meta.get('id', id_)}")
            print(f"        Distance: {dist:.4f}")
            print(f"        Doc: {doc[:100]}...")
    else:
        print(f"   ✗ No results returned")
        
except Exception as e:
    print(f"   ✗ Embedding test failed: {e}")
    import traceback
    traceback.print_exc()

# Check vector documents parquet
print(f"\n7. Checking vector documents parquet...")
vector_docs_path = Path("data/processed/vector_documents.parquet")
if vector_docs_path.exists():
    import pandas as pd
    try:
        df = pd.read_parquet(vector_docs_path)
        print(f"   ✓ vector_documents.parquet exists")
        print(f"   Rows: {len(df)}")
        print(f"   Columns: {df.columns.tolist()}")
    except Exception as e:
        print(f"   ✗ Failed to read parquet: {e}")
else:
    print(f"   ✗ vector_documents.parquet doesn't exist")

print("\n" + "="*70)
print("DIAGNOSTIC COMPLETE")
print("="*70 + "\n")
