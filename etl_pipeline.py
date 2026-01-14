import duckdb
import time

con = duckdb.connect()

# FILE NAMES
file_works = 'ol_dump_works_2025-12-31.txt.gz' 
file_editions = 'ol_dump_editions_2025-12-31.txt.gz' 

print("🚀 Attempt #4: Fixing the key column...")
start = time.time()

# 1. EDITIONS TABLE
editions_query = f"""
SELECT 
    json_extract_string(column4, '$.works[0].key') AS work_key,
    regexp_extract(json_extract_string(column4, '$.publish_date'), '(\\d{{4}})') AS year
FROM read_csv(
    '{file_editions}', 
    header=False, 
    delim='\\t', 
    quote='', 
    escape='', 
    all_varchar=True, 
    max_line_size=10000000
)
WHERE work_key IS NOT NULL AND year IS NOT NULL
"""

# 2. WORKS TABLE
works_query = f"""
SELECT 
    column1 AS work_key, 
    json_extract_string(column4, '$.title') AS title
FROM read_csv(
    '{file_works}', 
    header=False, 
    delim='\\t', 
    quote='', 
    escape='', 
    all_varchar=True,
    max_line_size=10000000
)
"""

# 3. JOIN 
final_query = f"""
WITH 
    raw_editions AS ({editions_query}),
    raw_works AS ({works_query})

SELECT 
    e.year,
    COUNT(*) as books_count
FROM raw_editions e
JOIN raw_works w ON e.work_key = w.work_key
GROUP BY e.year
ORDER BY e.year DESC
"""

try:
    print("⏳ Processing data...")
    df = con.execute(final_query).df()
    end = time.time()

    print(f"✅ SUCCESS! Execution time: {end - start:.4f} sec.")
    print(df.head(20))
    
    df.to_csv("final_timeline.csv", index=False)
    print("📄 Results saved to final_timeline.csv")

except Exception as e:
    print("❌ Error:", e)