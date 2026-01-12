import duckdb
import json

con = duckdb.connect()
filename = 'ol_dump_works_2025-12-31.txt.gz'

print("🕵️‍♂️ Ищем улики (выводим 5 книг красиво)...")

# Берем 5 случайных книг, у которых описание подлиннее (значит книга популярная)
# Используем length(column4) > 200 как простой фильтр мусора
query = f"""
SELECT column4
FROM read_csv(
    '{filename}', 
    header=False, 
    delim='\\t', 
    quote='', 
    escape=''
) 
WHERE length(column4) > 200 
LIMIT 5
"""

# Получаем список строк
results = con.execute(query).fetchall()

for row in results:
    raw_json_string = row[0]
    
    # Превращаем строку в Python-словарь
    parsed_json = json.loads(raw_json_string)
    
    # Самая главная команда: indent=4 делает отступы
    print(json.dumps(parsed_json, indent=4, ensure_ascii=False))
    print("-" * 40) # Разделитель