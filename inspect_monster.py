import duckdb
import json

con = duckdb.connect()
filename = 'ol_dump_editions_2025-12-31.txt.gz' 

# The "monster" record key
monster_key = '/books/OL59216279M'

print(f"🔬 Dissecting record {monster_key}...")

# Get the raw JSON specifically for this book
query = f"""
SELECT column4
FROM read_csv(
    '{filename}', 
    header=False, 
    delim='\\t', 
    quote='', 
    escape='', 
    all_varchar=True, 
    max_line_size=20000000 -- Essential for large records!
) 
WHERE column1 = '{monster_key}'
"""

raw_json = con.execute(query).fetchone()

if raw_json:
    data = json.loads(raw_json[0])
    
    print("\n--- FIELD ANALYSIS ---")
    # Iterating through all fields to identify the "heavy" ones
    for key, value in data.items():
        # Convert the value to a string to measure its size in bytes
        str_val = str(value)
        size_mb = len(str_val) / 1024 / 1024
        
        print(f"Field '{key}': {size_mb:.4f} MB")
        
        if size_mb > 1:
            print(f"⚠️ FOUND IT! The culprit is field: '{key}'")
            print("Preview (first 100 characters):")
            print(str_val[:100] + "...")
            
            if "data:image" in str_val[:100]:
                print("💡 DIAGNOSIS: This is a Base64 encoded image! Someone embedded the file directly.")
            else:
                print("💡 DIAGNOSIS: This looks like a massive block of text or raw data.")
else:
    print("❌ Book not found. Please check the monster_key or the filename.")