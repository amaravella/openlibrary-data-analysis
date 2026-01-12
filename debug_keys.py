import duckdb

con = duckdb.connect()

# ПРОВЕРЬ ИМЕНА ФАЙЛОВ!
file_works = 'ol_dump_works_2025-12-31.txt.gz' 
file_editions = 'ol_dump_editions_2025-12-31.txt.gz'

print("🕵️‍♂️ НАЧИНАЕМ СЛЕДСТВИЕ...")

print("\n--- 1. ПРОВЕРКА ТРУДОВ (WORKS) ---")
print("Смотрим, что лежит во 2-й колонке (наш ключ):")
# Читаем 5 строк, берем 2-ю колонку
works_check = con.execute(f"""
    SELECT column2 
    FROM read_csv('{file_works}', header=False, delim='\\t', quote='', escape='', all_varchar=True, max_line_size=10000000)
    LIMIT 5
""").df()
print(works_check)


print("\n--- 2. ПРОВЕРКА ИЗДАНИЙ (EDITIONS) ---")
print("Смотрим, удается ли достать ключ труда из JSON:")
# Пытаемся вытащить work_key тем же способом, что в большом скрипте
editions_check = con.execute(f"""
    SELECT 
        json_extract_string(column4, '$.works[0].key') as extracted_key,
        column4 as raw_json_preview -- Посмотрим и на сырой JSON, если экстракция вернет None
    FROM read_csv('{file_editions}', header=False, delim='\\t', quote='', escape='', all_varchar=True, max_line_size=10000000)
    WHERE column4 LIKE '%works%' -- Берем только те, где точно есть слово works
    LIMIT 5
""").df()
print(editions_check[['extracted_key']]) # Покажем только ключ

print("\n--- 3. ПРОВЕРКА ДАТЫ (EDITIONS) ---")
print("Смотрим, как выглядят даты и работает ли регулярка:")
date_check = con.execute(f"""
    SELECT 
        json_extract_string(column4, '$.publish_date') as raw_date,
        regexp_extract(json_extract_string(column4, '$.publish_date'), '(\\d{{4}})') as extracted_year
    FROM read_csv('{file_editions}', header=False, delim='\\t', quote='', escape='', all_varchar=True, max_line_size=10000000)
    WHERE column4 LIKE '%publish_date%'
    LIMIT 10
""").df()
print(date_check)