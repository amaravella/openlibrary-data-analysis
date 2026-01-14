import duckdb
import time

con = duckdb.connect()
filename = 'ol_dump_editions_2025-12-31.txt.gz' 

print("🦖 Releasing the Kraken... Searching for the heaviest records.")
print("Reading the entire file, this might take a minute.")
start = time.time()

# SQL QUERY
# 1. length(column4) - calculates character count.
# 2. Divide by 1024*1024 to convert to megabytes.
# 3. substr(column4, 1, 200) - take the first 200 characters to see the content,
#    without overloading the terminal.

query = f"""
SELECT 
    (length(column4) / 1024.0 / 1024.0) AS Size_MB,
    json_extract_string(column4, '$.title') AS Title,
    column1 AS Key,
    substr(column4, 1, 200) AS Preview_Start
FROM read_csv(
    '{filename}', 
    header=False, 
    delim='\\t', 
    quote='', 
    escape='', 
    all_varchar=True, 
    max_line_size=20000000 -- Setting to 20MB to ensure everything fits
) 
ORDER BY length(column4) DESC
LIMIT 5;
"""

df = con.execute(query).df()
end = time.time()

print(f"✅ Found in {end - start:.4f} sec.")
print("\n--- TOP 5 HEAVIEST RECORDS ---")
print(df)

# If you want to see the champion's "insides" completely
top_content_query = f"""
SELECT column4 
FROM read_csv('{filename}', header=False, delim='\\t', quote='', escape='', all_varchar=True, max_line_size=20000000) 
ORDER BY length(column4) DESC 
LIMIT 1
"""

print("\n--- INSIDE THE CHAMPION (First 1000 characters) ---")
champion = con.execute(top_content_query).fetchone()[0]
print(champion[:1000] + "...\n[REMAINDER TRUNCATED]")