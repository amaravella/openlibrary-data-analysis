import duckdb

con = duckdb.connect()
filename = 'ol_dump_works_2025-12-31.txt.gz'

print("🕵️‍♂️ Смотрим на внутренности одной книги...")

# Достаем просто сырой JSON, чтобы прочитать его глазами
query = f"""
SELECT column4
FROM read_csv(
    '{filename}', 
    header=False, 
    delim='\\t', 
    quote='', 
    escape=''
) 
LIMIT 1
"""

result = con.execute(query).fetchone()
print(result[0])